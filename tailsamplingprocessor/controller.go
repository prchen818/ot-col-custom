package tailsamplingprocessor

// PID 控制器结构体
// 目标：让实际采样率逼近 config.SampleRate
// 采样率调整范围 [0,1]
type PIDController struct {
	Kp, Ki, Kd float64
	prevError  float64
	integral   float64
}

func NewPIDController(kp, ki, kd float64) *PIDController {
	return &PIDController{Kp: kp, Ki: ki, Kd: kd}
}

func (pid *PIDController) Update(target, actual float64) float64 {
	error := target - actual
	pid.integral += error
	derivative := error - pid.prevError
	pid.prevError = error
	output := pid.Kp*error + pid.Ki*pid.integral + pid.Kd*derivative
	if output < 0 {
		output = 0
	}
	if output > 1 {
		output = 1
	}
	return output
}

type FlowController struct {
}
