package model

type Message struct {
	Name    string
	Id      int64
	Data    interface{}
	Retries int
	Status  MessageStatus
}

type MessageStatus string

var (
	Success   MessageStatus = "success"
	Fail      MessageStatus = "fail"
	Scheduled MessageStatus = "scheduled"
)
