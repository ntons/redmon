package remon

type SyncOnSaveFunc func(key string, version int64)
type SyncOnErrorFunc func(err error) error

type syncHook struct {
	onSave  SyncOnSaveFunc
	onError SyncOnErrorFunc
}

type SyncHooker interface {
	apply(h *syncHook)
}

type funcSyncHooker struct {
	f func(h *syncHook)
}

func (f funcSyncHooker) apply(h *syncHook) {
	f.f(h)
}

func SyncOnSave(f SyncOnSaveFunc) SyncHooker {
	return funcSyncHooker{func(h *syncHook) { h.onSave = f }}
}

func SyncOnError(f SyncOnErrorFunc) SyncHooker {
	return funcSyncHooker{func(h *syncHook) { h.onError = f }}
}
