package mqtt

import (
	"math/rand"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

const (
	publishPromNamespace = "mqtt_loadtest_publisher"
	charset              = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	publishedMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: publishPromNamespace,
		Name:      "messages_total",
		Help:      "How many messages were send",
	},
		[]string{"publisher", "topic"},
	)
	connectedPublishers = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: publishPromNamespace,
		Name:      "connected_publishers_total",
		Help:      "How many publishers are active",
	})
)

type PublishConfig struct {
	URL               string
	TopicPrefix       string
	TopicCount        uint
	PublisherPrefix   string
	PublisherCount    uint
	ProtocolVersion   uint
	QoSType           uint8
	CleanSession      bool
	ConnectDelay      time.Duration
	ChurnRate         time.Duration
	MessageRate       time.Duration
	PrometheusPath    string
	PrometheusEnabled bool
	ListenAddr        string
	User              string
	Password          string
}

func StringWithCharset(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func NewDefaultPublisherPool(conf PublishConfig) PublisherPool {
	pubPool := PublisherPool{
		TopicPrefix: conf.TopicPrefix,
	}
	opt := mqtt.NewClientOptions().SetProtocolVersion(conf.ProtocolVersion).AddBroker(conf.URL)
	if conf.User != "" {
		opt.SetUsername(conf.User)
		opt.SetPassword(conf.Password)
	}
	for idx := uint(0); idx < conf.PublisherCount; idx++ {
		opt.SetClientID(conf.PublisherPrefix + strconv.Itoa(int(idx))).SetCleanSession(conf.CleanSession)
		pubPool.clients = append(pubPool.clients, mqtt.NewClient(opt))
	}
	return pubPool
}

type PublisherPool struct {
	clients     []mqtt.Client
	TopicPrefix string
}

func (pubs *PublisherPool) Connect(topicCount uint, connectDelay time.Duration) error {
	for idx, pub := range pubs.clients {
		time.Sleep(connectDelay)
		log.Debugf("Connecting with publisher %v", idx)
		if token := pub.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}
		connectedPublishers.Inc()
	}
	return nil
}

func (pubs *PublisherPool) Publish(qos byte, topicCount uint, connectDelay, messageRate time.Duration) error {
	for pubIdx, pub := range pubs.clients {
		for topicIdx := uint(0); topicIdx < topicCount; topicIdx++ {
			log.Debugf("Starting publishing for publisher %v in topic %v", pubIdx, topicIdx)
			routineIdx := topicIdx
			go func() {
				sum := 0
				for {
					log.Debugf("Publishing message %v from publisher %v to topic %v", sum, pubIdx, pubs.TopicPrefix+strconv.Itoa(int(routineIdx)*(pubIdx+1)))
					pub.Publish(pubs.TopicPrefix+strconv.Itoa(int(routineIdx)*(pubIdx+1)), qos, false, StringWithCharset(20))
					publishedMessages.WithLabelValues(strconv.Itoa(int(pubIdx)), pubs.TopicPrefix+strconv.Itoa(int(routineIdx)*(pubIdx+1))).Inc()
					sum++
					time.Sleep(time.Duration(rand.Int63n(int64(messageRate) * 2)))
				}
			}()
		}
	}
	return nil
}
