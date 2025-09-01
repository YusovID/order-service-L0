// Package slogpretty предоставляет кастомный обработчик (handler) для стандартного
// логгера `slog`, который форматирует вывод в удобном для человека,
// цветном виде. Это особенно полезно для локальной разработки, так как
// стандартный JSON-вывод трудно читать.
package slogpretty

import (
	"context"
	"encoding/json"
	"io"
	stdLog "log"
	"log/slog"
	"os"

	"github.com/fatih/color"
)

// Константы для определения среды выполнения.
// Используются в `SetupLogger` для выбора подходящего формата логов.
const (
	envLocal = "local" // Локальная разработка (цветной вывод).
	envDev   = "dev"   // Среда разработки (JSON, debug уровень).
	envProd  = "prod"  // Продакшен-среда (JSON, info уровень).
)

// PrettyHandlerOptions содержит опции для настройки PrettyHandler.
type PrettyHandlerOptions struct {
	SlogOpts *slog.HandlerOptions // Стандартные опции slog, например, уровень логирования.
}

// PrettyHandler - это реализация `slog.Handler`, которая форматирует
// лог-записи в цветном, читаемом виде.
type PrettyHandler struct {
	opts PrettyHandlerOptions
	slog.Handler
	l     *stdLog.Logger // Используется стандартный `log` для вывода, чтобы избежать рекурсии.
	attrs []slog.Attr    // Атрибуты, добавленные через `WithAttrs`.
}

// NewPrettyHandler создает новый экземпляр PrettyHandler.
func (opts PrettyHandlerOptions) NewPrettyHandler(
	out io.Writer,
) *PrettyHandler {
	h := &PrettyHandler{
		// Внутренний JSON-обработчик можно использовать для делегирования
		// некоторых стандартных функций, хотя в данном `Handle` он не используется напрямую.
		Handler: slog.NewJSONHandler(out, opts.SlogOpts),
		// Создаем `log.Logger` для прямого вывода в `out` без префиксов.
		l: stdLog.New(out, "", 0),
	}
	return h
}

// SetupLogger является фабрикой логгеров. Она создает и возвращает
// `*slog.Logger` с подходящим обработчиком в зависимости от переданной
// строки окружения (`env`).
func SetupLogger(env string) *slog.Logger {
	var log *slog.Logger

	switch env {
	case envLocal:
		// Для локальной разработки используем наш красивый цветной логгер.
		log = setupPrettySlog()
	case envDev:
		// Для dev-окружения — стандартный JSON с уровнем Debug.
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envProd:
		// Для продакшена — стандартный JSON с уровнем Info.
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)
	}
	return log
}

// setupPrettySlog — вспомогательная функция для инкапсуляции
// создания и настройки PrettyHandler.
func setupPrettySlog() *slog.Logger {
	opts := PrettyHandlerOptions{
		SlogOpts: &slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	}
	handler := opts.NewPrettyHandler(os.Stdout)
	return slog.New(handler)
}

// Handle — основной метод обработчика, который вызывается для каждой записи лога.
// Он форматирует запись `slog.Record` и выводит ее в `io.Writer`.
func (h *PrettyHandler) Handle(_ context.Context, r slog.Record) error {
	// Форматируем уровень лога и добавляем цвет.
	level := r.Level.String() + ":"
	switch r.Level {
	case slog.LevelDebug:
		level = color.MagentaString(level)
	case slog.LevelInfo:
		level = color.BlueString(level)
	case slog.LevelWarn:
		level = color.YellowString(level)
	case slog.LevelError:
		level = color.RedString(level)
	}

	// Собираем все атрибуты (из записи и глобальные) в одну мапу.
	fields := make(map[string]interface{}, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		fields[a.Key] = a.Value.Any()
		return true
	})
	for _, a := range h.attrs {
		fields[a.Key] = a.Value.Any()
	}

	// Маршалим атрибуты в отформатированный JSON.
	var b []byte
	var err error
	if len(fields) > 0 {
		b, err = json.MarshalIndent(fields, "", "  ")
		if err != nil {
			return err
		}
	}

	// Форматируем время и сообщение.
	timeStr := r.Time.Format("[15:05:05.000]")
	msg := color.CyanString(r.Message)

	// Выводим итоговую строку с помощью `stdLog.Logger`.
	h.l.Println(
		timeStr,
		level,
		msg,
		color.WhiteString(string(b)),
	)

	return nil
}

// WithAttrs создает новый экземпляр PrettyHandler с добавленными атрибутами.
// Эти атрибуты будут добавляться ко всем последующим записям лога.
func (h *PrettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &PrettyHandler{
		Handler: h.Handler,
		l:       h.l,
		attrs:   append(h.attrs, attrs...),
	}
}

// WithGroup создает новый обработчик с именем группы.
// TODO: Текущая реализация не форматирует группы особым образом,
// а просто делегирует вызов нижележащему обработчику.
func (h *PrettyHandler) WithGroup(name string) slog.Handler {
	// TODO: implement
	return &PrettyHandler{
		Handler: h.Handler.WithGroup(name),
		l:       h.l,
		attrs:   h.attrs, // Группы не обрабатываются визуально, атрибуты просто наследуются.
	}
}
