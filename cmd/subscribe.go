package cmd

import (
	"net/http"
	"time"

	"github.com/freenowtech/mqtt-loadtest/mqtt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var SubscribeConfig mqtt.SubscribeConfig

func init() {
	subscribeCmd.Flags().StringVarP(&SubscribeConfig.URL, "url", "u", "", "URL to the MQTT broker instance")
	subscribeCmd.MarkFlagRequired("url")
	subscribeCmd.Flags().StringVarP(&SubscribeConfig.TopicPrefix, "topic-prefix", "p", "mqtt-loadtest", "Prefix which will form the topic name together with the index of the topic count ( `loadtest0`, `loadtest4`)")
	subscribeCmd.Flags().UintVarP(&SubscribeConfig.Subscriber, "subscriber", "s", 1, "Count of subscribers; One can subscribe to multiple topics")
	subscribeCmd.Flags().UintVarP(&SubscribeConfig.TopicsPerSubscriber, "topics-per-subscriber", "t", 1, "Count of Topics a subscriber subscribes to")
	subscribeCmd.Flags().UintVar(&SubscribeConfig.ProtocolVersion, "protocol-version", 4, "MQTT protocol version (3, 4(3.1.1) and 5 supported)")
	subscribeCmd.Flags().StringVar(&SubscribeConfig.SubscriberPrefix, "subscriber-prefix", "mqtt-loadtest-sub", "Prefix which will form the subscriber client id together with the index of the subscriber count (`loadtest-subscriber0`, `loadtest-subscriber4`")
	subscribeCmd.Flags().DurationVarP(&SubscribeConfig.ConnectDelay, "connect-delay", "d", time.Millisecond*200, "Delay before connecting the next subscriber for the initial connection build up")
	subscribeCmd.Flags().DurationVarP(&SubscribeConfig.ChurnRate, "churn-rate", "r", time.Second*1, "Delay between churns; On churn the subscriber will unsubscribe, disconnect, connect and subscribe")
	subscribeCmd.Flags().BoolVar(&SubscribeConfig.CleanSession, "clean-session", true, "Clean session flag for MQTT connections")
	subscribeCmd.Flags().StringVar(&SubscribeConfig.PrometheusPath, "prometheus-path", "/metrics", "Path on which Prometheus can scrape (`/metrics`, `/mqtt-loadtest/metrics`)")
	subscribeCmd.Flags().BoolVar(&SubscribeConfig.PrometheusEnabled, "prometheus-enabled", false, "Whether prometheus metrics should be exported or not")
	subscribeCmd.Flags().StringVarP(&SubscribeConfig.ListenAddr, "listen-address", "l", ":8080", "Address on which metrics and health will be served (`127.0.0.1:8081`, `:8081`)")
	subscribeCmd.Flags().StringVar(&SubscribeConfig.User, "user", "", "MQTT user")
	subscribeCmd.Flags().StringVar(&SubscribeConfig.Password, "password", "", "MQTT password")
	rootCmd.AddCommand(subscribeCmd)
}

var subscribeCmd = &cobra.Command{
	Use:   "subscribe",
	Short: "Subscribes to a specified topic with a certain amount of subscribers",
	RunE: func(cmd *cobra.Command, args []string) error {
		if SubscribeConfig.PrometheusEnabled {
			http.Handle(SubscribeConfig.PrometheusPath, promhttp.Handler())
			http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			log.Infof("Serving metrics at %v %v", SubscribeConfig.ListenAddr, SubscribeConfig.PrometheusPath)
			go http.ListenAndServe(SubscribeConfig.ListenAddr, nil)
		}

		subs, err := mqtt.NewDefaultSubscriberPool(SubscribeConfig)
		if err != nil {
			return err
		}
		log.Infof("Subscribing %v subscriber with prefix %v", SubscribeConfig.Subscriber, SubscribeConfig.TopicPrefix)
		log.Infof("Starting to listen to %v", SubscribeConfig.URL)
		err = subs.Subscribe(SubscribeConfig.TopicPrefix, SubscribeConfig.ConnectDelay, SubscribeConfig.TopicsPerSubscriber)
		if err != nil {
			return err
		}
		log.Infof("Starting to subscribe continuously")
		subs.KeepSubscribing(SubscribeConfig.TopicsPerSubscriber)

		log.Infof("Starting churning at rate 1 per %v", SubscribeConfig.ChurnRate)
		return subs.Churn(SubscribeConfig.ChurnRate, SubscribeConfig.ConnectDelay, SubscribeConfig.TopicsPerSubscriber)
	},
}
