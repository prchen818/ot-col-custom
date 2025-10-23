package tailsamplingprocessor

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestPID(t *testing.T) {
	controller := &Controller{
		Kp:        0.5,
		Ki:        0.06,
		Kd:        0.01,
		prevError: 0,
		integral:  0,
	}

	targetSampleRate := 0.8
	currentSampleRate := 0.1

	fmt.Println("Target Sample Rate:", targetSampleRate)
	fmt.Println("------------------------------------")

	for i := 0; i < 30; i++ {
		adjustment := controller.Update(targetSampleRate, currentSampleRate)
		noise := rand.Float64()*0.1 - 0.05 // [-0.1, 0.1]
		currentSampleRate += adjustment + noise
		// Clamp the sample rate to be within [0, 1]
		if currentSampleRate < 0 {
			currentSampleRate = 0
		}
		if currentSampleRate > 1 {
			currentSampleRate = 1
		}

		fmt.Printf("Step %d: Current Rate=%.4f, Adjustment=%.4f, Error=%.4f\n", i+1, currentSampleRate, adjustment, targetSampleRate-currentSampleRate)
	}

}
