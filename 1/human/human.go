package human

import (
	"errors"
	"fmt"
)

// Data was taken form web
const (
	MaxAge    = 150
	MaxWeight = 700
	MinWeight = 2
	MaxHeight = 300
	MinHeight = 50
)

var (
	ErrEmptyName = errors.New("name can't be empty")

	ErrAgeOverflow = fmt.Errorf("age can't be more than %d years", MaxAge)

	ErrWeightOverflow  = fmt.Errorf("weight can't be more than %d kg", MaxWeight)
	ErrWeightUnderflow = fmt.Errorf("weight can't be less than %d kg", MinWeight)
	ErrNegativeWeight  = errors.New("weight can't be negative")

	ErrHeightOverflow  = fmt.Errorf("height can't be more than %d cm", MaxHeight)
	ErrHeightUnderflow = fmt.Errorf("height can't be less than %d cm", MinHeight)
)

// Aliases used for validation and typing
type Name string
type Age uint8
type Height uint
type Weight uint

type Human struct {
	Name   Name
	Age    Age
	Height Height
	Weight Weight
}

// Function New takes age type uint8,
// height and weight type uint and returns pointer on struct Human.
func New(name Name, age Age, height Height, weight Weight) (*Human, error) {
	err := validateArgs(name, age, height, weight)
	if err != nil {
		return nil, fmt.Errorf("validation falied: %v", err)
	}

	return &Human{
		Name:   name,
		Age:    age,
		Height: height,
		Weight: weight,
	}, nil
}

// Function Birthday increases h.Age by 1.
func (h *Human) Birthday() error {
	if h.Age+1 > MaxAge {
		return ErrAgeOverflow
	}

	h.Age++

	fmt.Printf("%s celebrated her birthday!\n%s is %d y.o.\n", h.Name, h.Name, h.Age)

	return nil
}

// Function LoseWeight reduces h.Weight on value 'mass'.
// It will return the error if value of mass is invalid.
func (h *Human) LoseWeight(mass Weight) error {
	if mass >= h.Weight {
		return ErrNegativeWeight
	}
	if h.Weight-mass < MinWeight {
		return ErrWeightUnderflow
	}

	h.Weight -= mass

	fmt.Printf("%s losed weight\n%s's weight is %d kg\n", h.Name, h.Name, h.Weight)

	return nil
}

// Function GainWeight gains h.Weight on value 'mass'.
func (h *Human) GainWeight(mass Weight) error {
	if h.Weight+mass > MaxWeight {
		return ErrWeightOverflow
	}

	h.Weight += mass

	fmt.Printf("%s gained weight\n%s's weight is %d kg\n", h.Name, h.Name, h.Weight)

	return nil
}

// Function String formats information about 'h'
func (h *Human) String() string {
	return fmt.Sprintf("%s is %d y.o., height is %d cm, weight is %d kg", h.Name, h.Age, h.Height, h.Weight)
}

// Function validateArgs validate bunch of arguments,
// that was given to it
//
// It can validate type Name, Age, Weight and Height
func validateArgs(args ...any) error {
	errs := make([]error, 0, len(args))

	for _, arg := range args {
		switch v := arg.(type) {
		case Name:
			if v == "" {
				errs = append(errs, ErrEmptyName)
			}

		case Age:
			if v > MaxAge {
				errs = append(errs, ErrAgeOverflow)
			}

		case Weight:
			if v > MaxWeight {
				errs = append(errs, ErrWeightOverflow)
			}
			if v < MinWeight {
				errs = append(errs, ErrWeightUnderflow)
			}

		case Height:
			if v > MaxHeight {
				errs = append(errs, ErrHeightOverflow)
			}
			if v < MinHeight {
				errs = append(errs, ErrHeightUnderflow)
			}

		default:
			errs = append(errs, fmt.Errorf("unknown type was given: %T", v))
		}
	}

	return errors.Join(errs...)
}
