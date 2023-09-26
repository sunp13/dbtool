package dbtool

import (
	"io"
	"os"

	"github.com/rs/zerolog"
)

var (
	L zerolog.Logger
)

// 更新writer, 如果不调用,默认使用os.Stdout
func SetLogger(w io.Writer) {
	L = zerolog.New(os.Stdout)
	if w != nil {
		L = L.Output(w)
	}
	L = L.With().Timestamp().Logger().With().Caller().Logger().With().Str("module", "dbtool").Logger()
}
