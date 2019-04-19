package kafkaRest

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/elastic/beats/libbeat/outputs/transport"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/gofrs/uuid"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
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
	method             string
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
	method            string
}

// Client is an kafka rest client.
type Client struct {
	Connection
	tlsConfig *transport.TLSConfig
	topic     string
	pipeline  *outil.Selector
	params    map[string]string
	timeout   time.Duration

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
	//var dialer, tlsDialer transport.Dialer
	//var encoder bodyEncoder
	client := &Client{
		Connection: Connection{
			URL:      s.URL,
			Username: s.Username,
			Password: s.Password,
			Headers:  s.Headers,
			http: &http.Client{
				Transport: &http.Transport{
					//Dial:    dialer.Dial,
					//DialTLS: tlsDialer.Dial,
				},
				Timeout: s.Timeout,
			},
			method: s.method,
			//encoder: encoder,
		},
		tlsConfig: s.TLS,
		topic:     s.Topic,
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

func dataEncode(
	data []publisher.Event,
) []io.Reader {
	//okEvents := list.New()
	//okEvents := data[:0]
	var okEvents []io.Reader
	var message string
	for i := range data {
		event := &data[i].Content
		byteEvent, err := json.Marshal(event)
		if err != nil {
			logp.Err("Failed to encode event: %s", err)
			logp.Debug("kafka rest", "Failed event: %v", event)
			continue
		}
		if i == (len(data) - 1) {
			message = message + `{"value":` + string(byteEvent) + `}`
		} else {
			message = message + `{"value":` + string(byteEvent) + `},`
		}

	}
	message = `{"records":[` + message + `]}`
	jsonEvent := strings.NewReader(message)
	okEvents = append(okEvents, jsonEvent)
	return okEvents
}

// PublishEvents sends all events to elasticsearch. On error a slice with all
// events not published or confirmed to be processed by elasticsearch will be
// returned. The input slice backing memory will be reused by return the value.
func (client *Client) publishEvents(
	data []publisher.Event,
) ([]publisher.Event, error) {

	//begin := time.Now()
	st := client.observer
	//[]byte(data[0])
	if st != nil {
		st.NewBatch(len(data))
	}

	if len(data) == 0 {
		return nil, nil
	}
	okevent := dataEncode(data)
	for i := range okevent {
		tem := okevent[i]
		err := sendRequests(tem, client.URL, client.Username, client.Password)
		if err != nil {
			fmt.Errorf("publish event fail: %v", err)
		}
	}

	return nil, nil
}

func sendRequests(
	recodes io.Reader,
	url string,
	username string,
	password string) error {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
	}
	req, err := http.NewRequest("POST", url, recodes)

	if err != nil {
		fmt.Println("req fail")
	}
	tem := username + ":" + password
	Authorization := []byte(tem)
	str := "Basic " + base64.StdEncoding.EncodeToString(Authorization)
	req.Header.Add("Authorization", str)
	req.Header.Add("Content-Type", "application/vnd.kafka.json.v2+json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("kafka rest 请求失败:", err)
		return err
	}
	resp.Body.Close()
	return nil
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
		return fmt.Errorf("Connection marked as failed because the onConnect callback failed:", err)
	}
	return nil
}

// Close closes a connection.
func (conn *Connection) Close() error {
	return nil
}
