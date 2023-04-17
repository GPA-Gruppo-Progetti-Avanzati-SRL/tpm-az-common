package cosopsutil

type Options struct {
	PageSize    int
	Concurrency int
}

type Option func(opts *Options)

var PagedReaderDefaultOptions = Options{
	PageSize:    500,
	Concurrency: 1,
}

var DeleteAllDefaultOptions = Options{
	PageSize:    500,
	Concurrency: 1,
}

var DefaultPipelineOptions = DeleteAllDefaultOptions

func WithPageSize(s int) Option {
	return func(opts *Options) {
		opts.PageSize = s
	}
}

func WithConcurrency(s int) Option {
	return func(opts *Options) {
		opts.Concurrency = s
	}
}
