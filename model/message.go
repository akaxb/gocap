package model

import "time"

type Message struct {
	Headers map[string]string
	//Name    string
	Id    int64       `json:"id"`
	Value interface{} `json:"value"`
}

type MediumMessage struct {
	Id        int64      `json:"id"`
	Content   string     `json:"content"`
	Name      string     `json:"name"`
	Added     time.Time  `json:"added"`
	ExpiresAt *time.Time `json:"expiresAt"`
	Retries   int        `json:"retries"`
	Origin    Message    `json:"origin"`
}

type MessageStatus string

var (
	Success   MessageStatus = "Succeeded"
	Fail      MessageStatus = "Failed"
	Scheduled MessageStatus = "Scheduled"
)
