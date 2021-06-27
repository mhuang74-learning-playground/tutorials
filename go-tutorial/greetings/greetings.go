package greetings

import "fmt"
import "errors"
import "math/rand"
import "time"

// Hello returns a greeting for the named person
func Hello(name string) (string, error) {
    // give error if no name given
    if name == "" {
        return "", errors.New("empty name")
    }
    // Return a greeting that embeds the name in a message
    message := fmt.Sprintf(randomFormat(), name)
    return message, nil
}

// Hellos returns a map of greetings
func Hellos(names []string) (map[string]string, error) {
    // map to associate name to greeting
    messages := make(map[string]string)
    
    for _, name := range names {
        message, err := Hello(name)
        if err != nil {
            return nil, err
        }
        messages[name] = message
    }

    return messages, nil
}

func init() {
    rand.Seed(time.Now().UnixNano())
}

func randomFormat() string {
    formats := []string {
        "Hi, %v. Welcome!",
        "Great to see you, %v!",
        "很高興見到你, %v!",
    }

    return formats[rand.Intn(len(formats))]
}
