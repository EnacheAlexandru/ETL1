package main

import (
	"ETL1/sources"
	"fmt"
)

func main() {
	const filenameInput string = "resources/input1.csv"
	const chunkSize int = 3

	err := sources.Extract(filenameInput, chunkSize)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("ok")
}
