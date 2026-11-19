package envelope

import "errors"

type Message struct {
	From, FromSession, FromName, FromMode string
	HopChain                              []string
	Body                                  string
}

func Render(message Message) (string, error) { return "", errors.ErrUnsupported }
func Parse(content string) (Message, error)  { return Message{}, errors.ErrUnsupported }
func SelfCheck() error                       { return errors.ErrUnsupported }
