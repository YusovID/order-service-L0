// Package logger предоставляет кастомный middleware для логирования HTTP-запросов.
// Он использует структурированный логгер slog для вывода подробной информации
// о каждом входящем запросе и его результате.
package logger

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// New создает и возвращает новый middleware для chi, который логирует запросы.
//
// Middleware оборачивает каждый HTTP-запрос, записывая в лог следующую информацию:
//   - В момент получения запроса: метод, путь, удаленный адрес, user-agent и ID запроса.
//   - После завершения обработки: статус ответа, количество записанных байт и общую длительность обработки.
//
// Параметры:
//   - log: экземпляр slog.Logger, который будет использоваться для записи логов.
//
// Возвращает функцию, соответствующую сигнатуре chi middleware.
func New(log *slog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		// Добавляем к логгеру атрибут "component", чтобы легко фильтровать
		// логи, относящиеся к этому конкретному middleware.
		log := log.With(
			slog.String("component", "middleware/logger"),
		)

		log.Info("logger middleware enabled")

		// fn - это основная функция-обработчик, которая будет выполняться для каждого запроса.
		fn := func(w http.ResponseWriter, r *http.Request) {
			// Создаем запись лога (entry) с атрибутами, специфичными для текущего запроса.
			entry := log.With(
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("remote_addr", r.RemoteAddr),
				slog.String("user_agent", r.UserAgent()),
				slog.String("request_id", middleware.GetReqID(r.Context())),
			)

			// middleware.NewWrapResponseWriter - это специальная обертка для http.ResponseWriter,
			// которая позволяет перехватывать и сохранять информацию о статусе ответа и
			// количестве записанных байт. Стандартный http.ResponseWriter этого не позволяет.
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			// Засекаем время начала обработки запроса.
			t1 := time.Now()

			// defer гарантирует, что итоговая запись в лог будет сделана
			// после того, как следующий обработчик в цепочке (next.ServeHTTP) завершит свою работу.
			defer func() {
				entry.Info("request completed",
					slog.Int("status", ww.Status()),
					slog.Int("bytes", ww.BytesWritten()),
					slog.String("duration", time.Since(t1).String()),
				)
			}()

			// Передаем управление следующему обработчику в цепочке middleware.
			next.ServeHTTP(ww, r)
		}

		// Возвращаем созданный обработчик как http.HandlerFunc.
		return http.HandlerFunc(fn)
	}
}
