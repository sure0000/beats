package kafkaRest

import (
	"github.com/elastic/beats/libbeat/common/transport/tlscommon"
	"time"
)

type kafkaRestConfig struct {
	Protocol         string            `config:"protocol"`
	Path             string            `config:"path"`
	Params           map[string]string `config:"parameters"`
	Headers          map[string]string `config:"headers"`
	Username         string            `config:"username"`
	Password         string            `config:"password"`
	ProxyURL         string            `config:"proxy_url"`
	LoadBalance      bool              `config:"loadbalance"`
	CompressionLevel int               `config:"compression_level" validate:"min=0, max=9"`
	EscapeHTML       bool              `config:"escape_html"`
	TLS              *tlscommon.Config `config:"ssl"`
	BulkMaxSize      int               `config:"bulk_max_size"`
	MaxRetries       int               `config:"max_retries"`
	Timeout          time.Duration     `config:"timeout"`
	Backoff          Backoff           `config:"backoff"`
	topic            string            `config:"topic"`
}

type Backoff struct {
	Init time.Duration
	Max  time.Duration
}

var (
	defaultConfig = kafkaRestConfig{
		Protocol:         "",
		Path:             "",
		ProxyURL:         "",
		Params:           nil,
		Username:         "",
		Password:         "",
		Timeout:          90 * time.Second,
		MaxRetries:       3,
		CompressionLevel: 0,
		EscapeHTML:       true,
		TLS:              nil,
		LoadBalance:      true,
		Backoff: Backoff{
			Init: 1 * time.Second,
			Max:  60 * time.Second,
		},
		topic: "",
	}
)
