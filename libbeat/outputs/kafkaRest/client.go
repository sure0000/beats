package kafkaRest

import (
	"fmt"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/elastic/beats/libbeat/outputs/transport"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/gofrs/uuid"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type connectCallback func(client *Client) error

// Callbacks must not depend on the result of a previous one,
// because the ordering is not fixed.
type callbacksRegistry struct {
	callbacks map[uuid.UUID]connectCallback
	mutex     sync.Mutex
}

// ClientSettings contains the settings for a client.
type ClientSettings struct {
	URL                string
	Proxy              *url.URL
	TLS                *transport.TLSConfig
	Username, Password string
	EscapeHTML         bool
	Parameters         map[string]string
	Headers            map[string]string
	Topic              string
	Pipeline           *outil.Selector
	Timeout            time.Duration
	CompressionLevel   int
	Observer           outputs.Observer
}

// Connection manages the connection for a given client.
type Connection struct {
	URL      string
	Username string
	Password string
	Headers  map[string]string

	http              *http.Client
	onConnectCallback func() error
	version           string
}

// Client is an elasticsearch client.
type Client struct {
	Connection
	tlsConfig *transport.TLSConfig

	topic    string
	pipeline *outil.Selector
	params   map[string]string
	timeout  time.Duration

	// buffered json response reader
	//json jsonReader

	// additional configs
	compressionLevel int
	proxyURL         *url.URL

	observer outputs.Observer
}

func NewClient(
	s ClientSettings,
	onConnect *callbacksRegistry,
) (*Client, error) {
	proxy := http.ProxyFromEnvironment
	if s.Proxy != nil {
		proxy = http.ProxyURL(s.Proxy)
	}

	pipeline := s.Pipeline
	if pipeline != nil && pipeline.IsEmpty() {
		pipeline = nil
	}

	// TODO: add socks5 proxy support
	var dialer, tlsDialer transport.Dialer
	var err error

	dialer = transport.NetDialer(s.Timeout)
	tlsDialer, err = transport.TLSDialer(dialer, s.TLS, s.Timeout)
	if err != nil {
		return nil, err
	}

	if st := s.Observer; st != nil {
		dialer = transport.StatsDialer(dialer, st)
		tlsDialer = transport.StatsDialer(tlsDialer, st)
	}

	params := s.Parameters

	client := &Client{
		Connection: Connection{
			URL:      s.URL,
			Username: s.Username,
			Password: s.Password,
			Headers:  s.Headers,
			http: &http.Client{
				Transport: &http.Transport{
					Dial:    dialer.Dial,
					DialTLS: tlsDialer.Dial,
					Proxy:   proxy,
				},
				Timeout: s.Timeout,
			},
		},
		tlsConfig: s.TLS,
		topic:     s.Topic,
		pipeline:  pipeline,
		params:    params,
		timeout:   s.Timeout,
		proxyURL:  s.Proxy,
		observer:  s.Observer,
	}

	client.Connection.onConnectCallback = func() error {
		if onConnect != nil {
			onConnect.mutex.Lock()
			defer onConnect.mutex.Unlock()

			for _, callback := range onConnect.callbacks {
				err := callback(client)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	return client, nil

}

// Clone clones a client.
func (client *Client) Clone() *Client {
	// when cloning the connection callback and params are not copied. A
	// client's close is for example generated for topology-map support. With params
	// most likely containing the ingest node pipeline and default callback trying to
	// create install a template, we don't want these to be included in the clone.

	c, _ := NewClient(
		ClientSettings{
			URL:              client.URL,
			Topic:            client.topic,
			Proxy:            client.proxyURL,
			TLS:              client.tlsConfig,
			Username:         client.Username,
			Password:         client.Password,
			Parameters:       nil, // XXX: do not pass params?
			Headers:          client.Headers,
			Timeout:          client.http.Timeout,
			CompressionLevel: client.compressionLevel,
		},
		nil, // XXX: do not pass connection callback?
	)
	return c
}

func (client *Client) Publish(batch publisher.Batch) error {
	events := batch.Events()
	rest, err := client.publishEvents(events)
	if len(rest) == 0 {
		batch.ACK()
	} else {
		batch.RetryEvents(rest)
	}
	return err
}

// PublishEvents sends all events to elasticsearch. On error a slice with all
// events not published or confirmed to be processed by elasticsearch will be
// returned. The input slice backing memory will be reused by return the value.
func (client *Client) publishEvents(
	data []publisher.Event,
) ([]publisher.Event, error) {
	return nil, nil
}

func (client *Client) String() string {
	return "kafka rest(" + client.Connection.URL + ")"
}

// Connect connects the client.
func (conn *Connection) Connect() error {
	var err error
	//conn.version, err = conn.Ping()
	//if err != nil {
	//	return err
	//}

	err = conn.onConnectCallback()
	if err != nil {
		return fmt.Errorf("Connection marked as failed because the onConnect callback failed: %v", err)
	}
	return nil
}

// Close closes a connection.
func (conn *Connection) Close() error {
	return nil
}
