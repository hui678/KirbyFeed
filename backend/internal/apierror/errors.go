package apierror

import (
	"errors"
	"net/http"

	"gorm.io/gorm"
)

var (
	ErrUnauthorized = errors.New("unauthorized")
	ErrValidation   = errors.New("validation error")

	// ErrServiceOverloaded 表示服务主动丢弃了本次请求以保护下游（如回源并发
	// 已达上限）。它应当映射为 503 而不是 500：这是「限流/降级」而非「故障」，
	// 客户端收到 503 可以退避重试，收到 500 则通常不会重试。
	ErrServiceOverloaded = errors.New("service overloaded")
)

func ClassifyHTTPStatus(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, ErrValidation):
		return http.StatusBadRequest
	case errors.Is(err, ErrServiceOverloaded):
		return http.StatusServiceUnavailable
	case errors.Is(err, gorm.ErrRecordNotFound):
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}
