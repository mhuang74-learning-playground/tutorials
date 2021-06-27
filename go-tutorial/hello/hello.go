package main

import "fmt"
import "log"
import "example.com/greetings"

func main() {
    log.SetPrefix("greetings: ")
    log.SetFlags(0)

    names := []string{"Charis", "Zhachary", "Tina", "Michael"}

    // get greetings and print
    message, err := greetings.Hellos(names)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println(message)
}

