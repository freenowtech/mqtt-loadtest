package cmd

import (
	"net/http"
	"time"

	"github.com/freenowtech/mqtt-loadtest/mqtt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var PublishConfig mqtt.PublishConfig

func init() {
	publishCmd.Flags().StringVarP(&PublishConfig.URL, "url", "u", "", "URL to the MQTT broker instance (`127.0.0.1:1883`, `localhost:1883`)")
	publishCmd.MarkFlagRequired("url")
	publishCmd.Flags().StringVarP(&PublishConfig.TopicPrefix, "topic-prefix", "p", "mqtt-loadtest", "Prefix which will form the topic name together with the index of the topic count ( `loadtest0`, `loadtest4`)")
	publishCmd.Flags().UintVarP(&PublishConfig.TopicCount, "topic-count", "c", 10, "Count of topics to publish to per publisher")
	publishCmd.Flags().UintVar(&PublishConfig.ProtocolVersion, "protocol-version", 4, "MQTT protocol version (3, 4(3.1.1) and 5 supported)")
	publishCmd.Flags().Uint8VarP(&PublishConfig.QoSType, "qos-type", "q", 0, "The MQTT QoS type to use (0 - at most once,1 - at least once and 2 - exactly once)")
	publishCmd.Flags().StringVar(&PublishConfig.PublisherPrefix, "publisher-prefix", "mqtt-loadtest-pub", "Prefix which will form the publishers client id together with the index of the publisher count (`loadtest-publisher0`, `loadtest-publisher4`")
	publishCmd.Flags().UintVar(&PublishConfig.PublisherCount, "publisher-count", 1, "Count of publishers")
	publishCmd.Flags().DurationVarP(&PublishConfig.ConnectDelay, "connect-delay", "d", time.Millisecond*200, "Delay before connecting the next publisher for the initial connection build up")
	publishCmd.Flags().DurationVarP(&PublishConfig.MessageRate, "message-rate", "m", time.Second*1, "Delay between messages get send out; Distributed to avoid bulk messages")
	publishCmd.Flags().BoolVar(&PublishConfig.CleanSession, "clean-session", true, "Clean session flag for MQTT connections")
	publishCmd.Flags().StringVar(&PublishConfig.PrometheusPath, "prometheus-path", "/metrics", "Path on which Prometheus can scrape (`/metrics`, `/mqtt-loadtest/metrics`)")
	publishCmd.Flags().BoolVar(&PublishConfig.PrometheusEnabled, "prometheus-enabled", false, "Whether prometheus metrics should be exported or not")
	publishCmd.Flags().StringVarP(&PublishConfig.ListenAddr, "listen-address", "l", ":8080", "Address on which metrics and health will be served (`127.0.0.1:8081`, `:8081`)")
	publishCmd.Flags().StringVar(&PublishConfig.User, "user", "", "MQTT user")
	publishCmd.Flags().StringVar(&PublishConfig.Password, "password", "", "MQTT password")
	rootCmd.AddCommand(publishCmd)
}

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Publishes to a specified topic with a certain amount of publishers and a publish rate",
	RunE: func(cmd *cobra.Command, args []string) error {
		if PublishConfig.PrometheusEnabled {
			http.Handle(PublishConfig.PrometheusPath, promhttp.Handler())
			http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			log.Infof("Serving metrics at %v %v", PublishConfig.ListenAddr, PublishConfig.PrometheusPath)
			go http.ListenAndServe(PublishConfig.ListenAddr, nil)
		}
		pubs := mqtt.NewDefaultPublisherPool(PublishConfig)
		log.Infof("Publishing %v publishers to %v topics with prefix %v", PublishConfig.PublisherCount, PublishConfig.TopicCount, PublishConfig.TopicPrefix)
		err := pubs.Connect(PublishConfig.TopicCount, PublishConfig.ConnectDelay)
		if err != nil {
			return err
		}
		log.Infof("Starting to publish to %v", PublishConfig.URL)
		err = pubs.Publish(byte(PublishConfig.QoSType), PublishConfig.TopicCount, PublishConfig.ConnectDelay, PublishConfig.MessageRate)
		if err != nil {
			return err
		}
		select {}
	},
}
