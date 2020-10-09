package mqtt

import (
	"fmt"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

const subscribePromNamespace = "mqtt_loadtest_subscriber"
var keepSubscribingWaitTime = 10*time.Second
var keepSubscribingDelay = 1*time.Second

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
	URL                 string
	TopicPrefix         string
	TopicsPerSubscriber uint
	ProtocolVersion     uint
	SubscriberPrefix    string
	ConnectDelay        time.Duration
	ChurnRate           time.Duration
	CleanSession        bool
	PrometheusEnabled   bool
	PrometheusPath      string
	ListenAddr          string
	Subscriber          uint
	User                string
	Password            string
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

	for idx := uint(0); idx < conf.Subscriber; idx++ {
		opt.SetClientID(conf.SubscriberPrefix + strconv.Itoa(int(idx)))
		opt.SetCleanSession(conf.CleanSession)
		opt.SetConnectTimeout(50 * time.Second)
		opt.SetPingTimeout(50 * time.Second)
		opt.SetKeepAlive(50 * time.Second)
		//opt.SetDefaultPublishHandler(createPublishHandler("defaultPublishHandler"))
		subPool.clients = append(subPool.clients, mqtt.NewClient(opt))
	}
	return subPool, nil
}

type SubscriberPool struct {
	clients     []mqtt.Client
	TopicPrefix string
}

func (subs *SubscriberPool) KeepSubscribing(topicsPerSubscriber uint) {
	for cidx, clt := range subs.clients {
		go func(){
			for {
				time.Sleep(keepSubscribingWaitTime)
				// TODO connectedSubscribers.Inc()
				for idx := uint(0); idx < topicsPerSubscriber; idx++ {
					topic := fmt.Sprintf("%s%d", subs.TopicPrefix, uint(cidx)*topicsPerSubscriber+idx+2)
					defer func() {
						if err := recover(); err != nil {
							log.Debugf("Subscribing to %v", topic)
							if token := clt.Subscribe(topic, 0, createPublishHandler(topic)); token.Wait() && token.Error() != nil {
								log.Errorf(token.Error().Error())
							}
						}
					}()
					//clt.Unsubscribe(topic)
					time.Sleep(keepSubscribingDelay)
					log.Debugf("Subscribing to %v", topic)
					if token := clt.Subscribe(topic, 0, createPublishHandler(topic)); token.Wait() && token.Error() != nil {
						log.Errorf(token.Error().Error())
					}
				}

			}
		}()
	}
}

func (subs *SubscriberPool) Subscribe(topicPrefix string, connectDelay time.Duration, topicsPerSubscriber uint) error {
	fmt.Println(len(subs.clients))
	for cidx, clt := range subs.clients {
		time.Sleep(connectDelay)
		if token := clt.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}
		connectedSubscribers.Inc()

		for idx := uint(0); idx < topicsPerSubscriber; idx++ {
			topic := fmt.Sprintf("%s%d", subs.TopicPrefix, uint(cidx)*topicsPerSubscriber+idx)
			log.Debugf("Subscribing to %v", topic)
			if token := clt.Subscribe(topic, 0, createPublishHandler(topic)); token.Wait() && token.Error() != nil {
				return token.Error()
			}
		}
	}
	return nil
}

func (subs *SubscriberPool) Churn(churnRate, connectDelay time.Duration, topicsPerSubscriber uint) error {
	for {
		for cidx, clt := range subs.clients {
			log.Debugf("Churning subscriber %v", cidx)
			go func() {
				for idx := uint(0); idx < topicsPerSubscriber; idx++ {
					topic := fmt.Sprintf("%s%d", subs.TopicPrefix, uint(cidx)*topicsPerSubscriber+idx)
					clt.Unsubscribe(topic)
				}
				clt.Disconnect(0)
				oldOpts := clt.OptionsReader()
				opts := mqtt.NewClientOptions().SetUsername(oldOpts.Username()).SetPassword(oldOpts.Password()).SetClientID(oldOpts.ClientID()).SetProtocolVersion(oldOpts.ProtocolVersion()).AddBroker(oldOpts.Servers()[0].String()).SetCleanSession(oldOpts.CleanSession()).SetConnectTimeout(oldOpts.ConnectTimeout()).SetPingTimeout(oldOpts.PingTimeout()).SetKeepAlive(oldOpts.KeepAlive())
				//opts.SetDefaultPublishHandler(createPublishHandler(subs.TopicPrefix + strconv.Itoa(idx)))
				newClt := mqtt.NewClient(opts)
				subs.clients[cidx] = newClt

				time.Sleep(connectDelay)
				if token := newClt.Connect(); token.Wait() && token.Error() != nil {
					log.Error(token.Error())
					churningErrors.Inc()
					return
				}

				for idx := uint(0); idx < topicsPerSubscriber; idx++ {
					topic := fmt.Sprintf("%s%d", subs.TopicPrefix, uint(cidx)*topicsPerSubscriber+idx)
					log.Debugf("Subscribing to %v", topic)
					if token := newClt.Subscribe(topic, 0, createPublishHandler(topic)); token.Wait() && token.Error() != nil {
						log.Error(token.Error())
						churningErrors.Inc()
					}
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
