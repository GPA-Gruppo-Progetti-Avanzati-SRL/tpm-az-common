package cosopsutil

type Options struct {
	PageSize int
}

type Option func(opts *Options)

var PagedReaderDefaultOptions = Options{
	PageSize: 500,
}

var DeleteAllDefaultOptions = Options{
	PageSize: 500,
}

func WithPageSize(s int) Option {
	return func(opts *Options) {
		opts.PageSize = s
	}
}
