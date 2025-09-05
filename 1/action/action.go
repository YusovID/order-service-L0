package action

import "level-1/1/human"

type Action struct {
	*human.Human // asserted type
}

// Function New takes pointer on struct Human
// and returns pointer on Action struct
func New(human *human.Human) *Action {
	return &Action{human}
}
