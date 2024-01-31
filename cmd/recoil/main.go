package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
)

// RecoilDirection calculates the recoil direction based on the value
func RecoilDirection(value float64) float64 {
	return math.Sin((value+5)*(math.Pi/10)) * (100 - value)
}

// SVGOutput generates the SVG based on the given value
func SVGOutput(value float64) string {
	verticalScale := 0.8
	maxSpread := 180.0 // degrees

	recoilDirectionRadians := RecoilDirection(value)
	direction := recoilDirectionRadians * verticalScale * (math.Pi / 180) // Convert to radians
	x := math.Sin(direction)
	y := math.Cos(direction)

	// how wide the recoil wedge is, this is half of the actual spread as we'll go in both directions
	spread := ((100 - value) / 100) * (maxSpread / 2) * (math.Pi / 180)

	// the point on the wedge clockwise from the recoil direction (right)
	xClockwise := math.Sin(direction + spread)
	yClockwise := math.Cos(direction + spread)

	// the point on the wedge counter clockwise from the recoil direction (left)
	xCounterClockwise := math.Sin(direction - spread)
	yCounterClockwise := math.Cos(direction - spread)

	svg := `<svg height="120" viewBox="0 0 2 1">
	<circle r="1" cx="1" cy="1" fill="#FFF" ></circle>
    `

	// if the value is 95 or higher, draw a line instead of a wedge
	if value >= 95 {
		svg += fmt.Sprintf(`<line x1="%.16f" y1="%.16f" x2="%.16f" y2="%.16f" stroke="#333" stroke-width="0.1" ></line>`,
			1-x, 1+y, 1+x, 1-y)
	} else {
		svg += fmt.Sprintf(
			`<path d="M1,1 L%.16f,%.16f A1,1 0 1,0 %.16f,%.16f Z" fill="#333" ></path>`,
			1+xClockwise,
			1-yClockwise,
			1+xCounterClockwise,
			1-yCounterClockwise,
		)
	}
	svg += `</svg>`

	return svg
}

// HTMLPage generates an HTML page that embeds the given SVG
func HTMLPage(svg string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
<title>Recoil SVG</title>
<style>
	body {
		background-color: #333;
		color: white;
		display: flex;
		justify-content: center;
		align-items: center;
		height: 100vh;
		margin: 0;
	}
</style>
</head>
<body>
	<div>
        %s
    </div>
</body>
</html>`, svg)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: recoil <value>")
		os.Exit(1)
	}

	value, err := strconv.ParseFloat(os.Args[1], 64)
	if err != nil {
		fmt.Printf("Invalid input: %s\n", err)
		os.Exit(1)
	}

	svg := SVGOutput(value)
	html := HTMLPage(svg)
	fmt.Println(html)
}
