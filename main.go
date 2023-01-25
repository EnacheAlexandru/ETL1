package main

import (
	"etl/fprocessing"
	"fmt"
)

func main() {
	const filenameInput string = "input0.csv"
	const filenamePath = "fprocessing/data/" + filenameInput
	const chunkPath = "fprocessing/data/chunk"
	const chunkSize int = 3

	err := fprocessing.Extract(filenamePath, chunkPath, chunkSize)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("ok")
}
