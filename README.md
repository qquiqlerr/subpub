# subpub

`subpub` — это простая и эффективная реализация паттерна Publisher-Subscriber на Go, сохраняющая порядок сообщений (FIFO) и обеспечивающая независимость производительности подписчиков.

## 📦 Возможности

- Поддержка множественных подписчиков на один subject
- Асинхронная доставка сообщений каждому подписчику
- FIFO-очередь для каждого подписчика
- Возможность отписки от subject
- Безопасная работа с goroutine

## 🔧 Установка

```bash
go get github.com/qquiqlerr/subpub
```

## 🚀 Быстрый старт

```go
package main

import (
	"fmt"
	"github.com/qquiqlerr/subpub"
	"time"
)

func main() {
	ps := subpub.NewSubPub()

	sub, _ := ps.Subscribe("example.topic", func(msg interface{}) {
		fmt.Println("Received:", msg)
	})
	
	_ = ps.Publish("example.topic", "Hello, world!")

	time.Sleep(100 * time.Millisecond) // Wait for message to be processed

	sub.Unsubscribe()
	_ = ps.Close(context.Background())
}
```

## 🧪 Тестирование

```bash
go test -v ./...
```
