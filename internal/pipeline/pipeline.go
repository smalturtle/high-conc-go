// Package pipeline 实现 AI 流式对话的生产者-消费者管道：对标 LoveApp.doChatwithStream 返回的 Flux<String>。
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Token 是流式输出的最小单元，等价于 Flux<String> 里的每个 chunk / SSE 的每个 data 事件。
type Token struct {
	ChatID  string
	Content string
	Err     error
	Done    bool // Done=true 相当于 Flux 的 onComplete 信号
}

// Generator 模拟真实的模型流式生成（替换为调用 dashscope/OpenAI 的 SDK 即可）。
type Generator func(ctx context.Context, chatID, userMessage string) []string

// Limiter 用带缓冲 channel 实现的信号量：相当于 Java 的 java.util.concurrent.Semaphore(permits)。
type Limiter struct {
	sem chan struct{}
}

func NewLimiter(permits int) *Limiter {
	return &Limiter{sem: make(chan struct{}, permits)}
}

// Acquire 带超时的获取许可，绝不无限阻塞（相当于 semaphore.tryAcquire(timeout, unit)）。
func (l *Limiter) Acquire(ctx context.Context, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("pipeline: 获取并发许可超时，当前系统繁忙")
	}
}

func (l *Limiter) Release() {
	select {
	case <-l.sem:
	default:
		// 防御：Release 多次不应 panic（不像 Java Semaphore.release() 会让 permits 变大，这里直接忽略多余释放）
	}
}

// Pipeline 负责把"限流 + 生产 + 可取消"串起来。
type Pipeline struct {
	limiter *Limiter
	gen     Generator
}

func New(maxConcurrent int, gen Generator) *Pipeline {
	return &Pipeline{limiter: NewLimiter(maxConcurrent), gen: gen}
}

// Stream 开启一次流式对话，返回一个只读 channel 给上层（HTTP handler）逐块转发给客户端。
// 核心防阻塞设计：
//  1. 生产者 goroutine 内部用 select 监听 ctx.Done()，客户端断开时立刻停止生产，不做无用功。
//  2. 输出 channel 带缓冲（如同 Flux 的 backpressure buffer），发送时 select+ctx 兜底，防止消费者不读导致生产者永久阻塞。
//  3. defer 保证 channel 一定被 close、许可一定被 release，无论正常结束/异常/取消。
func (p *Pipeline) Stream(ctx context.Context, chatID, userMessage string) (<-chan Token, error) {
	if err := p.limiter.Acquire(ctx, 3*time.Second); err != nil {
		return nil, fmt.Errorf("chatID=%s 限流拒绝: %w", chatID, err)
	}

	out := make(chan Token, 16) // 缓冲队列，吸收生产/消费速度差，相当于 BlockingQueue 做流量整形

	go func() {
		defer p.limiter.Release() // 相当于 try-finally 里的 semaphore.release()
		defer close(out)          // 相当于 Flux 的 onComplete，通知消费方 range 循环退出

		defer func() {
			if r := recover(); r != nil {
				p.trySend(ctx, out, Token{ChatID: chatID, Err: fmt.Errorf("生成异常: %v", r)})
			}
		}()

		chunks := p.gen(ctx, chatID, userMessage) // 真实场景这里替换为逐 token 调用模型 API
		for _, c := range chunks {
			select {
			case <-ctx.Done():
				return // 客户端已断开/请求超时，立刻停止生产，避免浪费 AI 调用额度
			default:
			}
			if !p.trySend(ctx, out, Token{ChatID: chatID, Content: c}) {
				return // 发送失败（消费方已走/ctx 已取消），提前退出
			}
		}
		p.trySend(ctx, out, Token{ChatID: chatID, Done: true})
	}()

	return out, nil
}

// trySend 带超时/取消的安全发送：杜绝往无人接收的 channel 死等（这是新手最容易写出死锁的地方）。
func (p *Pipeline) trySend(ctx context.Context, out chan<- Token, t Token) bool {
	select {
	case out <- t:
		return true
	case <-ctx.Done():
		return false
	case <-time.After(2 * time.Second):
		return false // 消费方长时间不读取，主动放弃，防止生产者协程泄漏
	}
}

// FanIn 把多路 channel 合并为一路：相当于 Java 里用 CompletableFuture.anyOf/多个 Future 汇总结果。
// 典型场景：JManus 多 ToolCallback 并行调用后汇总输出。
func FanIn(ctx context.Context, sources ...<-chan Token) <-chan Token {
	out := make(chan Token, 16)
	var wg sync.WaitGroup // 相当于 Java 的 CountDownLatch(sources数量)，等所有上游都关闭再关 out
	wg.Add(len(sources))

	for _, src := range sources {
		go func(ch <-chan Token) {
			defer wg.Done()
			for {
				select {
				case v, ok := <-ch:
					if !ok {
						return
					}
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(src)
	}

	go func() {
		wg.Wait() // 所有生产者都退出后，统一关闭汇总 channel
		close(out)
	}()
	return out
}
