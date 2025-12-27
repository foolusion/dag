package dag

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/cloudflare/backoff"
)

type Runner interface {
	Run(context.Context) error
}

func mergeContexts(ctxs ...context.Context) (context.Context, context.CancelFunc) {
	if len(ctxs) == 0 {
		return context.WithCancel(context.Background())
	}
	ctx, cancel := context.WithCancel(ctxs[0])
	var stops []func() bool
	for _, c := range ctxs[1:] {
		stop := context.AfterFunc(c, func() {
			cancel()
		})
		stops = append(stops, stop)
	}
	return ctx, func() {
		for _, stop := range stops {
			stop()
		}
		cancel()
	}
}

type work struct {
	name    string
	parents map[string]struct{}
	runner  Runner
	done    chan struct{}
}

type Graph struct {
	workDAG    map[string]*work
	sequential bool
	logger     *slog.Logger

	started   sync.Map
	completed sync.Map
	backoff   *backoff.Backoff
}

type GraphOption func(*Graph)

func WithSequential(sequential bool) GraphOption {
	return func(g *Graph) {
		g.sequential = sequential
	}
}

func WithLogger(logger *slog.Logger) GraphOption {
	return func(g *Graph) {
		if logger != nil {
			g.logger = logger
		}
	}
}

func NewGraph(opts ...GraphOption) *Graph {
	g := &Graph{
		workDAG: make(map[string]*work),
		logger:  slog.New(slog.DiscardHandler),
		backoff: backoff.New(5*time.Second, time.Millisecond),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

type WorkOption func(*work) error

func WithParents(parents ...string) WorkOption {
	return func(w *work) error {
		w.parents = make(map[string]struct{}, len(parents))
		for _, parent := range parents {
			w.parents[parent] = struct{}{}
		}
		return nil
	}
}

func WithRunner(runner Runner) WorkOption {
	return func(w *work) error {
		w.runner = runner
		return nil
	}
}

func (g *Graph) Add(name string, opts ...WorkOption) error {
	w := &work{
		name: name,
	}
	for _, opt := range opts {
		if err := opt(w); err != nil {
			return err
		}
	}
	if w.runner == nil {
		return fmt.Errorf("no Runner provided")
	}
	g.workDAG[w.name] = w
	return nil
}

func (g *Graph) Run(ctx context.Context) error {
	type dagData struct {
		ctx      context.Context
		cancel   context.CancelFunc
		children []string
	}

	var wg sync.WaitGroup

	startCh := make(chan struct{})
	go func() {
		defer close(startCh)
		var toStart []*work
		for _, w := range g.workDAG {
			toStart = append(toStart, w)
		}
		for len(toStart) > 0 {
			g.logger.Info("in start loop")
			select {
			case <-ctx.Done():
				g.logger.Info("start loop context canceled")
				return
			case <-time.After(g.backoff.Duration()):
			}
			w := toStart[0]
			toStart = toStart[1:]
			canStart := true
			parentContexts := []context.Context{ctx}
			for parent := range w.parents {
				if g.sequential {
					if _, ok := g.completed.Load(parent); !ok {
						g.logger.Info("sequential runner with incomplete parents", "name", w.name, "parent", parent)
						canStart = false
						break
					}
				} else {
					if s, ok := g.started.Load(parent); !ok {
						g.logger.Info("parallel runner with incomplete parents", "name", w.name, "parent", parent)
						canStart = false
						break
					} else {
						parentContexts = append(parentContexts, s.(dagData).ctx)
					}
				}
			}
			if !canStart {
				toStart = append(toStart, w)
				continue
			}

			// when parent context is canceled, cancel the child context.
			workCtx, cancel := mergeContexts(parentContexts...)
			w.done = make(chan struct{})
			go func() {
				g.logger.Info("starting runner", "name", w.name)
				err := w.runner.Run(workCtx)
				if err != nil {
					g.logger.Error("runner failed", "name", w.name, "error", err)
				}
				cancel()
				close(w.done)
			}()
			go func() {
				<-w.done
				g.logger.Info("runner completed", "name", w.name)
				g.completed.Store(w.name, struct{}{})
				g.started.Delete(w.name)
			}()
			wg.Go(func() {
			<-w.done
			g.logger.Info("wait group worker completed", "name", w.name)
		})

			var children []string
			for c, cw := range g.workDAG {
				if _, ok := cw.parents[w.name]; ok {
					children = append(children, c)
				}
			}
			g.started.Store(w.name, dagData{
				ctx:      workCtx,
				cancel:   cancel,
				children: children,
			})
		}
	}()

	completed := make(chan struct{})
	go func() {
		<-startCh
		wg.Wait()
		g.logger.Info("wait group completed")
		close(completed)
	}()

	select {
	case <-ctx.Done(): // cancel all jobs
		var toStop []string
		g.started.Range(func(key any, _ any) bool {
			toStop = append(toStop, key.(string))
			return true
		})
		g.logger.Info("main loop context canceled", "toStop", toStop)
		for len(toStop) > 0 {
			name := toStop[0]
			toStop = toStop[1:]
			canStop := true
			w, ok := g.workDAG[name]
			if !ok {
				log.Fatalf("wtf: %v is not in workDAG", name)
			}
			s, ok := g.started.Load(name)
			if !ok {
				continue
			}
			ss := s.(dagData)
			for _, child := range ss.children {
				if _, ok := g.started.Load(child); ok {
					canStop = false
					break
				}
			}
			if !canStop {
				toStop = append(toStop, name)
				continue
			}
			ss.cancel()
			<-w.done
			g.started.Delete(name)
		}
		return ctx.Err()
	case <-completed:
		return nil
	}
}
