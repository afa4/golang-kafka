package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func main() {
	createClientProcess(10)
}

func createClientProcess(numberOfRequests int) {
	invokeClientCommand := fmt.Sprintf("go run client/client.go %d", numberOfRequests)
	bash := exec.Command("bash", "-c", invokeClientCommand)
	output, err := bash.Output()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	outputStr := string(output)
	repliesDurations := strings.Split(outputStr, "\n")
	numberOfDurations := len(repliesDurations) - 1
	totalDuration := 0
	for i := 0; i < numberOfDurations; i++ {
		replyDuration, err := strconv.Atoi(repliesDurations[i])
		if err != nil {
			panic(err.Error())
		}
		totalDuration += replyDuration
	}
	fmt.Printf("mean %d\n", totalDuration/numberOfDurations)

}
