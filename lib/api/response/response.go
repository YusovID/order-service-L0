// Package response предоставляет утилиты для формирования стандартных
// JSON-ответов HTTP API. Использование этих хелперов обеспечивает
// единообразие формата ответов во всем приложении.
package response

import (
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
)

// Response - это базовая структура для всех JSON-ответов.
// Она содержит поле `status` ("OK" или "Error") и опциональное
// поле `error` с текстом ошибки.
type Response struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"` // `omitempty` скрывает поле, если оно пустое.
}

// Константы для стандартизации значений в поле `Status`.
const (
	StatusOK    = "OK"
	StatusError = "Error"
)

// OK создает и возвращает стандартный успешный ответ.
// Используется, когда операция завершилась без ошибок, но не
// требует возврата каких-либо данных.
func OK() Response {
	return Response{
		Status: StatusOK,
	}
}

// Error создает и возвращает стандартный ответ с ошибкой.
// Принимает сообщение об ошибке, которое будет включено в ответ.
func Error(msg string) Response {
	return Response{
		Status: StatusError,
		Error:  msg,
	}
}

// ValidationError форматирует ошибки валидации от `go-playground/validator`
// в читаемый для пользователя вид.
// Функция итерируется по всем ошибкам валидации и создает
// понятные сообщения для каждой из них.
func ValidationError(errs validator.ValidationErrors) Response {
	var errMsgs []string

	for _, err := range errs {
		switch err.ActualTag() {
		case "required":
			errMsgs = append(errMsgs, fmt.Sprintf("field %s is a required field", err.Field()))
		case "uuid":
			errMsgs = append(errMsgs, fmt.Sprintf("field %s is not a valid uuid", err.Field()))
		// Сюда можно добавлять обработку других тегов валидации.
		default:
			errMsgs = append(errMsgs, fmt.Sprintf("field %s is not valid", err.Field()))
		}
	}

	return Response{
		Status: StatusError,
		Error:  strings.Join(errMsgs, ", "),
	}
}
