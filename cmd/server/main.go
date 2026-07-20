// cmd/server 是可执行入口：把 pool + pipeline + session 串成一个 HTTP 服务，
// 对标 project-allin 里的 AIController（/ai/love_app/chat/sync、/sse、/sse/emitter）。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"project-allin-go/internal/pipeline"
	"project-allin-go/internal/pool"
	"project-allin-go/internal/session"
)

func main() {
	// rootCtx 是全局根上下文：收到 SIGINT/SIGTERM 后自动 cancel，
	// 相当于 Spring 容器关闭时广播 ContextClosedEvent，各 Bean 的 @PreDestroy 依次触发。
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	workerPool := pool.New(rootCtx, 32, 256) // 32 常驻 worker，256 积压队列，相当于固定大小线程池 + 有界队列
	registry := session.NewRegistry()
	pl := pipeline.New(8, fakeModelGenerator) // 最多 8 个并发流式会话，相当于 Semaphore(8) 限流

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth(workerPool))
	mux.HandleFunc("/ai/love_app/chat/sync", handleSync(workerPool, registry))
	mux.HandleFunc("/ai/love_app/chat/sse", handleSSE(pl, registry))

	srv := &http.Server{
		Addr:         ":8081",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // SSE 长连接场景禁用写超时，靠 ctx 取消控制生命周期
	}

	// 用独立 goroutine 跑 ListenAndServe，主 goroutine 负责等待退出信号——这是 Go server 的标准范式。
	serverErr := make(chan error, 1)
	go func() {
		log.Println("server listening on :8081")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-rootCtx.Done():
		log.Println("收到退出信号，开始优雅停机...")
	case err := <-serverErr:
		if err != nil {
			log.Printf("server 启动失败: %v", err)
		}
	}

	// 分阶段优雅退出：先停 HTTP 入口（不再接受新请求），再停业务协程池（等在飞任务跑完）。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown 出错: %v", err)
	}
	if err := workerPool.Shutdown(8 * time.Second); err != nil {
		log.Printf("worker pool shutdown 出错: %v", err)
	}
	log.Println("已完全退出")
}

func handleHealth(p *pool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		submitted, completed, panicked := p.Stats()
		fmt.Fprintf(w, "ok submitted=%d completed=%d panicked=%d\n", submitted, completed, panicked)
	}
}

// handleSync 对标 doChatwithLoveAppSync：一次性拿到完整回复。
// 用 SubmitAndWait 把"HTTP 请求-响应"映射为 Java 的 future.get(timeout)。
func handleSync(p *pool.Pool, reg *session.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chatID := r.URL.Query().Get("chatId")
		userMessage := r.URL.Query().Get("userMessage")
		if chatID == "" || userMessage == "" {
			http.Error(w, "chatId/userMessage 不能为空", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second) // 15s 硬超时，防止慢请求堆积拖垮服务
		defer cancel()

		sess := reg.GetOrCreate(chatID)
		sess.Append(session.Message{Role: "user", Content: userMessage, CreatedAt: time.Now()})

		val, err := p.SubmitAndWait(ctx, func(jobCtx context.Context) (any, error) {
			select {
			case <-jobCtx.Done():
				return nil, jobCtx.Err()
			case <-time.After(200 * time.Millisecond): // 模拟一次模型调用耗时
				reply := "echo: " + userMessage
				return reply, nil
			}
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reply := val.(string)
		sess.Append(session.Message{Role: "assistant", Content: reply, CreatedAt: time.Now()})
		fmt.Fprint(w, reply)
	}
}

// handleSSE 对标 doChatwithLoveAppSSEEmitter：逐 token 推送，客户端断开要立刻停止生产。
func handleSSE(pl *pipeline.Pipeline, reg *session.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chatID := r.URL.Query().Get("chatId")
		userMessage := r.URL.Query().Get("userMessage")
		if chatID == "" || userMessage == "" {
			http.Error(w, "chatId/userMessage 不能为空", http.StatusBadRequest)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// r.Context() 会在客户端断开连接时自动 cancel，天然对接我们管道里的 ctx.Done() 退出逻辑，
		// 这一点比 Java SseEmitter 手动 catch IOException 优雅得多。
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute) // 对标 new SseEmitter(300*1000L)
		defer cancel()

		reg.GetOrCreate(chatID).Append(session.Message{Role: "user", Content: userMessage, CreatedAt: time.Now()})

		tokens, err := pl.Stream(ctx, chatID, userMessage)
		if err != nil {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}

		var full strings.Builder
		for {
			select {
			case <-ctx.Done():
				return // 客户端断开或超时，直接返回，range 中的生产者也会在下一次 select 感知到 ctx.Done 退出
			case t, ok := <-tokens:
				if !ok {
					reg.GetOrCreate(chatID).Append(session.Message{Role: "assistant", Content: full.String(), CreatedAt: time.Now()})
					return
				}
				if t.Err != nil {
					fmt.Fprintf(w, "event: error\ndata: %s\n\n", t.Err.Error())
					flusher.Flush()
					return
				}
				if t.Done {
					fmt.Fprint(w, "event: done\ndata: [DONE]\n\n")
					flusher.Flush()
					continue
				}
				full.WriteString(t.Content)
				fmt.Fprintf(w, "data: %s\n\n", t.Content)
				flusher.Flush()
			}
		}
	}
}

// fakeModelGenerator 模拟逐 token 输出，真实场景替换为调用 dashscope/OpenAI 流式 API。
func fakeModelGenerator(ctx context.Context, chatID, userMessage string) []string {
	words := strings.Fields("这 是 一次 模拟 的 流式 AI 回复 内容 完毕")
	_ = ctx
	_ = chatID
	_ = userMessage
	return words
}
