package model

const defaultVersion = "v1"
const defaultSucceedMessageExpiredAfter = 3600
const defaultFailedMessageExpiredAfter = 86400

type CapOptions struct {
	SucceedMessageExpiredAfter int
	FailedMessageExpiredAfter  int
	Version                    string
}

type CapOption func(*CapOptions)

func NewCapOptions(opt ...CapOption) *CapOptions {
	c := &CapOptions{
		Version:                    defaultVersion,
		SucceedMessageExpiredAfter: defaultSucceedMessageExpiredAfter,
		FailedMessageExpiredAfter:  defaultFailedMessageExpiredAfter,
	}
	return c
}

func WithVersion(value string) CapOption {
	return func(c *CapOptions) {
		c.Version = value
	}
}

func WithSucceedMessageExpiredAfter(value int) CapOption {
	return func(c *CapOptions) {
		c.SucceedMessageExpiredAfter = value
	}
}

func WithFailedMessageExpiredAfter(value int) CapOption {
	return func(c *CapOptions) {
		c.FailedMessageExpiredAfter = value
	}
}
