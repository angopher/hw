package main

import (
    "fmt"
    "time"
    "math/rand"
)


func main() {
    str := "abcdefghijklmnopqrstuvwxyz"
    bytes := []byte(str)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 10000000; i++ {
        result := []byte{}
        length := r.Intn(100) + 1
        for j := 0; j < length ; j++ {
            result = append(result, bytes[r.Intn(len(bytes))])
        }
        fmt.Println(string(result))
        fmt.Println(string(result))
    }

    for i := 0; i < 200; i++ {
        fmt.Printf("a");
    }
}
