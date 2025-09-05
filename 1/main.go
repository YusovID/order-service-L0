// Задание:
// Дана структура Human (с произвольным набором полей и методов).

// Реализовать встраивание методов в структуре Action от родительской структуры Human (аналог наследования).

// Подсказка: используйте композицию (embedded struct), чтобы Action имел все методы Human.

package main

import (
	"fmt"
	"level-1/1/action"
	"level-1/1/human"
	"log"
)

// default values
var (
	name   human.Name   = "Alice"
	age    human.Age    = 18
	height human.Height = 170
	weight human.Weight = 70
)

func main() {
	h, err := human.New(name, age, height, weight)
	if err != nil { // if validation failed
		log.Fatalf("failed to create new human: %v\n", err)
	}
	a := action.New(h)

	// printing information about 'h'
	fmt.Println(a.String())

	err = a.Birthday()
	if err != nil {
		log.Fatalf("failed to celebrate birthday: %v\n", err)
	}

	err = a.GainWeight(12)
	if err != nil {
		log.Fatalf("failed to gain weight: %v\n", err)
	}

	if err := a.LoseWeight(4); err != nil {
		log.Fatalf("failed to lose weight: %v\n", err)
	}
}
