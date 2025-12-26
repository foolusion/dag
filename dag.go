package dag

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type ParentConfigFunc func(parent, key string) any

func NoParentConfig(_, _ string) any {
	return nil
}

type Runner interface {
	Setup(context.Context, ParentConfigFunc) Runner
	Run(context.Context) error
	RunConfig(string) any
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
	workDAG map[string]*work
}

func (g *Graph) Setup(_ context.Context, _ ParentConfigFunc) Runner {
	return g
}

func (g *Graph) RunConfig(string) any {
	return nil
}

func NewGraph() *Graph {
	return &Graph{
		workDAG: make(map[string]*work),
	}
}

type WorkOptions func(*work) error

func WithParents(parents ...string) WorkOptions {
	return func(w *work) error {
		w.parents = make(map[string]struct{}, len(parents))
		for _, parent := range parents {
			w.parents[parent] = struct{}{}
		}
		return nil
	}
}

func WithRunner(runner Runner) WorkOptions {
	return func(w *work) error {
		w.runner = runner
		return nil
	}
}

func (g *Graph) Add(name string, opts ...WorkOptions) error {
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
	started := map[string]dagData{}

	var toStart []*work

	for _, w := range g.workDAG {
		toStart = append(toStart, w)
	}

	for len(toStart) > 0 {
		w := toStart[0]
		toStart = toStart[1:]
		canStart := true
		parentContexts := []context.Context{ctx}
		for parent := range w.parents {
			if s, ok := started[parent]; !ok {
				canStart = false
				break
			} else {
				parentContexts = append(parentContexts, s.ctx)
			}
		}
		if !canStart {
			toStart = append(toStart, w)
			continue
		}

		// when parent context is canceled, cancel the child context.
		workCtx, cancel := mergeContexts(parentContexts...)
		w.runner = w.runner.Setup(workCtx, func(parent, key string) any {
			p, ok := g.workDAG[parent]
			if !ok {
				panic(fmt.Errorf("dag: parent %q does not exist", parent))
			}
			_, ok = started[parent]
			if !ok {
				panic(fmt.Sprintf("dag: parent %q has not been started, are you sure this is a parent?", parent))
			}
			return p.runner.RunConfig(key)
		})
		w.done = make(chan struct{})
		go func() {
			err := w.runner.Run(workCtx)
			if err != nil {
				log.Printf("runner failed: %v", err)
			}
			cancel()
			close(w.done)
		}()

		var children []string
		for c, cw := range g.workDAG {
			if _, ok := cw.parents[w.name]; ok {
				children = append(children, c)
			}
		}
		started[w.name] = dagData{
			ctx:      workCtx,
			cancel:   cancel,
			children: children,
		}
	}

	completed := make(chan struct{})
	var wg sync.WaitGroup
	for _, w := range g.workDAG {
		wg.Go(func() {
			<-w.done
		})
	}
	go func() {
		wg.Wait()
		close(completed)
	}()

	select {
	case <-ctx.Done(): // cancel all jobs
		var toStop []string
		for name := range started {
			toStop = append(toStop, name)
		}
		for len(toStop) > 0 {
			name := toStop[0]
			toStop = toStop[1:]
			canStop := true
			w, ok := g.workDAG[name]
			if !ok {
				log.Fatalf("wtf: %v is not in workDAG", name)
			}
			s, ok := started[name]
			if !ok {
				continue
			}
			for _, child := range s.children {
				if _, ok := started[child]; ok {
					canStop = false
					break
				}
			}
			if !canStop {
				toStop = append(toStop, name)
				continue
			}
			started[name].cancel()
			<-w.done
			delete(started, name)
		}
		return ctx.Err()
	case <-completed:
	}
	return nil
}
