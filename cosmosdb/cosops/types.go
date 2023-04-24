package cosops

type Visitor interface {
	Visit(phase string, df DataFrame) error
	Count() int
}

type NopVisitor struct {
	counter int
}

func (v *NopVisitor) Visit(phase string, df DataFrame) error {
	v.counter++
	return nil
}

func (v *NopVisitor) Count() int {
	return v.counter
}

type DataFrame struct {
	id   string
	pkey string
	err  error
}

type Options struct {
	PageSize      int
	Concurrency   int
	Visitor       Visitor
	IdFieldName   string
	PKeyFieldName string
}

type Option func(opts *Options)

var ReadAndVisitDefaultOptions = Options{
	PageSize:      500,
	Concurrency:   1,
	IdFieldName:   "id",
	PKeyFieldName: "pkey",
	Visitor:       &NopVisitor{},
}

func WithPageSize(s int) Option {
	return func(opts *Options) {
		opts.PageSize = s
	}
}

func WithVisitor(v Visitor) Option {
	return func(opts *Options) {
		opts.Visitor = v
	}
}

func WithConcurrency(s int) Option {
	return func(opts *Options) {
		opts.Concurrency = s
	}
}

func WithIdFieldName(n string) Option {
	return func(opts *Options) {
		opts.IdFieldName = n
	}
}

func WithPKeyFieldName(n string) Option {
	return func(opts *Options) {
		opts.PKeyFieldName = n
	}
}
