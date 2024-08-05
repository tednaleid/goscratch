package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

type WeatherResponse struct {
	Main struct {
		Temp float64 `json:"temp"`
	} `json:"main"`
}

func getAPIKey() (string, error) {
	apiKey, err := ioutil.ReadFile("/tmp/secret.conf")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(apiKey)), nil
}

func getCity() string {
	city := "Madrid,ES"
	if len(os.Args) > 1 {
		city = os.Args[1]
	}
	return city
}

func getWeather(apiKey string, city string) (float64, error) {
	resp, err := http.Get(fmt.Sprintf("http://api.openweathermap.org/data/2.5/weather?q=%s&appid=%s&units=metric", city, apiKey))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var weatherResponse WeatherResponse
	err = json.Unmarshal(body, &weatherResponse)
	if err != nil {
		return 0, err
	}

	return weatherResponse.Main.Temp, nil
}

func printTemperature(city string, temp float64) {
	fmt.Printf("The current temperature in %s is %.2f°C\n", city, temp)

	if temp < 10 {
		fmt.Println("It's cold outside, wear a coat!")
	}
}

func main() {
	apiKey, err := getAPIKey()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	city := getCity()

	temp, err := getWeather(apiKey, city)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	printTemperature(city, temp)
}
