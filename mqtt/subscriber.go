package mqtt

import (
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

const subscribePromNamespace = "mqtt_loadtest_subscriber"

var (
	subscribedMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: subscribePromNamespace,
		Name:      "messages_total",
		Help:      "Number of send messages",
	},
		[]string{"topic"},
	)
	connectedSubscribers = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: subscribePromNamespace,
		Name:      "connected_subscribers_total",
		Help:      "Number of connected Subscribers",
	})
	churnedSubscribers = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: subscribePromNamespace,
		Name:      "churned_connections_total",
		Help:      "Number of churned connections",
	})
	churningErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: subscribePromNamespace,
		Name:      "churning_errors_total",
		Help:      "Number of connection failures while churning",
	})
)

type SubscribeConfig struct {
	URL               string
	TopicPrefix       string
	TopicCount        uint
	ProtocolVersion   uint
	SubscriberPrefix  string
	ConnectDelay      time.Duration
	ChurnRate         time.Duration
	CleanSession      bool
	PrometheusEnabled bool
	PrometheusPath    string
	ListenAddr        string
	User              string
	Password          string
}

func NewDefaultSubscriberPool(conf SubscribeConfig) (SubscriberPool, error) {
	subPool := SubscriberPool{
		TopicPrefix: conf.TopicPrefix,
	}
	opt := mqtt.NewClientOptions().SetProtocolVersion(conf.ProtocolVersion).AddBroker(conf.URL)
	if conf.User != "" {
		opt.SetUsername(conf.User)
		opt.SetPassword(conf.Password)
	}
	for idx := uint(0); idx < conf.TopicCount; idx++ {
		opt.SetClientID(conf.SubscriberPrefix + strconv.Itoa(int(idx)))
		opt.SetCleanSession(conf.CleanSession)
		opt.SetDefaultPublishHandler(createPublishHandler(conf.TopicPrefix + strconv.Itoa(int(idx))))
		subPool.clients = append(subPool.clients, mqtt.NewClient(opt))
	}
	return subPool, nil
}

type SubscriberPool struct {
	clients     []mqtt.Client
	TopicPrefix string
}

func (subs *SubscriberPool) Subscribe(topicPrefix string, topicCount uint, connectDelay time.Duration) error {
	for idx, sub := range subs.clients {
		time.Sleep(connectDelay)
		if token := sub.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}
		connectedSubscribers.Inc()

		log.Debugf("Subscribing to %v%v", subs.TopicPrefix, idx)
		if token := sub.Subscribe(subs.TopicPrefix+strconv.Itoa(idx), 0, nil); token.Wait() && token.Error() != nil {
			return token.Error()
		}
	}
	return nil
}

func (subs *SubscriberPool) Churn(churnRate, connectDelay time.Duration) error {
	for {
		for idx, sub := range subs.clients {
			log.Debugf("Churning subscriber %v", idx)
			go func() {
				sub.Unsubscribe(subs.TopicPrefix + strconv.Itoa(idx))
				sub.Disconnect(0)
				oldOpts := sub.OptionsReader()
				opts := mqtt.NewClientOptions().SetUsername(oldOpts.Username()).SetPassword(oldOpts.Password()).SetClientID(oldOpts.ClientID()).SetProtocolVersion(oldOpts.ProtocolVersion()).AddBroker(oldOpts.Servers()[0].String()).SetCleanSession(oldOpts.CleanSession())
				opts.SetDefaultPublishHandler(createPublishHandler(subs.TopicPrefix + strconv.Itoa(idx)))
				newSub := mqtt.NewClient(opts)
				subs.clients[idx] = newSub

				time.Sleep(connectDelay)
				if token := newSub.Connect(); token.Wait() && token.Error() != nil {
					log.Error(token.Error())
					churningErrors.Inc()
					return
				}
				if token := newSub.Subscribe(subs.TopicPrefix+strconv.Itoa(idx), 0, nil); token.Wait() && token.Error() != nil {
					log.Error(token.Error())
					churningErrors.Inc()
				}
			}()
			churnedSubscribers.Inc()
			time.Sleep(churnRate)
		}
	}
}

func createPublishHandler(topic string) func(client mqtt.Client, msg mqtt.Message) {
	sum := 0
	return func(client mqtt.Client, msg mqtt.Message) {
		sum++
		subscribedMessages.WithLabelValues(topic).Inc()
		log.Debugf("Subscriber received message %v from topic %v: %+v", sum, topic, msg)
	}
}
