package locking

type Locking interface {
	Lock(key string) error
	Unlock(key string) error
}
