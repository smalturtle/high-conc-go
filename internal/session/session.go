// Package session 实现按 chatId 隔离的并发安全会话注册表：对标 Spring AI 的 ChatMemory（内存存储）。
package session

import (
	"sync"
	"time"
)

// Message 对应 Java 里的一条对话记录。
type Message struct {
	Role      string // "user" / "assistant"
	Content   string
	CreatedAt time.Time
}

// Session 单个 chatId 的会话状态；用独立的 RWMutex 做"细粒度锁"，
// 避免所有 chatId 竞争同一把全局锁（相当于 Java 里对每个 key 用 ConcurrentHashMap.compute 局部加锁的效果）。
type Session struct {
	mu       sync.RWMutex // 读多写少场景用 RWMutex，相当于 Java 的 ReentrantReadWriteLock
	messages []Message
	closed   bool
}

func (s *Session) Append(msg Message) {
	s.mu.Lock() // 相当于 lock.writeLock().lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, msg)
}

func (s *Session) History() []Message {
	s.mu.RLock() // 相当于 lock.readLock().lock()，允许多个 goroutine 并发读
	defer s.mu.RUnlock()
	cp := make([]Message, len(s.messages)) // 拷贝快照返回，防止调用方拿到底层切片后并发修改
	copy(cp, s.messages)
	return cp
}

// Registry 是全局会话表：相当于 Java 的 ConcurrentHashMap<String, Session>。
type Registry struct {
	mu       sync.RWMutex // 保护 map 结构本身（新增/删除 key），不是保护 Session 内部字段
	sessions map[string]*Session
	initOnce sync.Once // 相当于 Java 的懒加载单例双重检查锁，这里用于惰性初始化内部资源
}

func NewRegistry() *Registry {
	return &Registry{sessions: make(map[string]*Session)}
}

// GetOrCreate 是最容易写出竞态的地方：必须保证"检查是否存在 + 创建"是原子的（相当于 ConcurrentHashMap.computeIfAbsent）。
func (r *Registry) GetOrCreate(chatID string) *Session {
	r.mu.RLock()
	s, ok := r.sessions[chatID]
	r.mu.RUnlock()
	if ok {
		return s
	}

	r.mu.Lock() // 升级为写锁前，必须重新检查一遍（double-check），防止并发下重复创建覆盖数据
	defer r.mu.Unlock()
	if s, ok = r.sessions[chatID]; ok {
		return s
	}
	s = &Session{}
	r.sessions[chatID] = s
	return s
}

func (r *Registry) Delete(chatID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, chatID)
}

func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}
