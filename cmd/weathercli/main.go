package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

type WeatherData struct {
	Main struct {
		Temp float64 `json:"temp"`
	} `json:"main"`
}

func main() {
	apiKey := getAPIKey()
	resp := getResponse(apiKey)
	data := parseResponse(resp)
	printTemperature(data)
}

func getAPIKey() string {
	apiKey := os.Getenv("OPENWEATHERMAP_API_KEY")
	if apiKey == "" {
		fmt.Println("The OPENWEATHERMAP_API_KEY environment variable is not set")
		os.Exit(1)
	}
	return apiKey
}

func getResponse(apiKey string) *http.Response {
	resp, err := http.Get(fmt.Sprintf("http://api.openweathermap.org/data/2.5/weather?q=Madrid,es&units=metric&appid=%s", apiKey))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return resp
}

func parseResponse(resp *http.Response) WeatherData {
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var data WeatherData
	err = json.Unmarshal(body, &data)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return data
}

func printTemperature(data WeatherData) {
	fmt.Printf("The temperature in Madrid is %.2f°C\n", data.Main.Temp)
}
