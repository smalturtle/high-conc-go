// Package pool 实现工业级 Goroutine 池：对标 Java ThreadPoolExecutor。
package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed    = errors.New("pool: 已关闭，拒绝新任务") // 相当于 RejectedExecutionException
	ErrQueueFull     = errors.New("pool: 队列已满，快速失败") // 相当于 CallerRunsPolicy/AbortPolicy 触发点
	ErrSubmitTimeout = errors.New("pool: 提交任务超时")
)

// Job 是一个可取消的任务单元，必须感知 ctx 及时退出，否则会造成 goroutine 泄漏。
type Job func(ctx context.Context) (any, error)

// task 内部结构：携带结果回传通道，模拟 Java 的 Future/CompletableFuture。
type task struct {
	job    Job
	result chan<- Result // 单向只写，调用方拿到只读端，编译期防误用
}

type Result struct {
	Val any
	Err error
}

// Pool 是固定大小的工作协程池。
type Pool struct {
	queue     chan task
	wg        sync.WaitGroup // 相当于 Java 的 CountDownLatch/线程池 awaitTermination 的基础
	ctx       context.Context
	cancel    context.CancelFunc
	closed    atomic.Bool // 相当于 AtomicBoolean，避免重复 Close 造成 panic: close of closed channel
	closeOnce sync.Once   // 相当于保证 shutdown 逻辑"只执行一次"的语义（类似 Java 的双重检查锁/单次初始化）

	// 统计字段，全部用 atomic 操作，避免对普通字段加锁（相当于 java.util.concurrent.atomic.*）
	submitted int64
	completed int64
	panicked  int64
}

// New 创建一个池：workers 为常驻 goroutine 数（相当于核心线程数），queueSize 为积压队列容量。
func New(parent context.Context, workers, queueSize int) *Pool {
	ctx, cancel := context.WithCancel(parent)
	p := &Pool{
		queue:  make(chan task, queueSize), // 有缓冲 channel = 阻塞队列 LinkedBlockingQueue
		ctx:    ctx,
		cancel: cancel,
	}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker(i)
	}
	return p
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()
	for {
		select { // select 相当于 Java 里的多路 IO 复用/带超时的 BlockingQueue.poll
		case <-p.ctx.Done():
			return // 收到取消信号，优雅退出，不再消费剩余队列，避免"僵尸协程"
		case t, ok := <-p.queue:
			if !ok {
				return // channel 已 close 且排空，等价于线程池 queue 消费完毕后退出
			}
			p.runSafely(t)
		}
	}
}

// runSafely 保证单个任务 panic 不会打垃圾整个 worker（相当于 Java 里 try/catch(Throwable) 兜底）。
func (p *Pool) runSafely(t task) {
	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&p.panicked, 1)
			if t.result != nil {
				t.result <- Result{Err: fmt.Errorf("panic recovered: %v", r)}
			}
		}
	}()
	val, err := t.job(p.ctx)
	atomic.AddInt64(&p.completed, 1)
	if t.result != nil {
		t.result <- Result{Val: val, Err: err}
	}
}

// Submit 异步提交，不等待结果（相当于 executor.execute(Runnable)）。
// 关键防阻塞点：用 select + ctx/timeout 兜底，绝不无脑 p.queue <- t 导致调用方永久阻塞。
func (p *Pool) Submit(job Job, timeout time.Duration) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}
	atomic.AddInt64(&p.submitted, 1)
	t := task{job: job}

	if timeout <= 0 {
		select {
		case p.queue <- t:
			return nil
		case <-p.ctx.Done():
			return ErrPoolClosed
		default:
			return ErrQueueFull // 队列满，快速失败而不是阻塞（相当于 AbortPolicy）
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case p.queue <- t:
		return nil
	case <-p.ctx.Done():
		return ErrPoolClosed
	case <-timer.C:
		return ErrSubmitTimeout // 相当于 offer(task, timeout, unit) 超时语义
	}
}

// SubmitAndWait 同步提交并等待结果（相当于 future.get(timeout)）。
func (p *Pool) SubmitAndWait(ctx context.Context, job Job) (any, error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}
	resultCh := make(chan Result, 1) // 缓冲为1，即使调用方提前放弃接收也不会让 worker 永久阻塞在发送上
	atomic.AddInt64(&p.submitted, 1)
	t := task{job: job, result: resultCh}

	select {
	case p.queue <- t:
	case <-p.ctx.Done():
		return nil, ErrPoolClosed
	case <-ctx.Done():
		return nil, ctx.Err() // 调用方自己的超时/取消，相当于 Future.get(timeout, unit) 抛 TimeoutException
	}

	select {
	case res := <-resultCh:
		return res.Val, res.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Shutdown 优雅停机：先停止接收新任务，再等待在飞任务跑完，超时后强制取消。
// 相当于 executorService.shutdown() + awaitTermination(timeout) + 超时后 shutdownNow()。
func (p *Pool) Shutdown(timeout time.Duration) error {
	var err error
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		close(p.queue) // 关闭队列：不再允许写入，但已入队任务仍可被 worker 消费完（排空语义）

		done := make(chan struct{})
		go func() {
			p.wg.Wait() // 等待所有 worker 的 defer wg.Done() 触发
			close(done)
		}()

		select {
		case <-done:
			// 所有 worker 正常退出
		case <-time.After(timeout):
			p.cancel() // 强制取消：广播 ctx.Done()，所有 select 中的 worker 立刻返回
			<-done     // 再等一次，确保真正退出（此时应该很快）
			err = errors.New("pool: 优雅退出超时，已强制取消所有任务")
		}
	})
	return err
}

// Stats 暴露原子计数器快照，供健康检查/监控埋点使用。
func (p *Pool) Stats() (submitted, completed, panicked int64) {
	return atomic.LoadInt64(&p.submitted), atomic.LoadInt64(&p.completed), atomic.LoadInt64(&p.panicked)
}
