package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer wg.Done()
			url := fmt.Sprintf("http://localhost:8083/publish/trans?msg=order-%d", id)
			resp, err := http.Get(url)
			if err != nil {
				fmt.Printf("get failed: %v", err)
			}
			defer resp.Body.Close()
			//read from resp
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("read failed: %v", err)
			}
			fmt.Println(string(data))
		}(i)
	}
	wg.Wait()
	fmt.Println("finished")
}
