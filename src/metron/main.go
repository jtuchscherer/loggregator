package main

import (
	"doppler/dopplerservice"
	"flag"
	"fmt"
	"path"
	"strings"
	"time"

	"metron/clientpool"
	"metron/networkreader"
	"metron/writers/dopplerforwarder"
	"metron/writers/eventmarshaller"
	"metron/writers/eventunmarshaller"
	"metron/writers/legacyunmarshaller"
	"metron/writers/messageaggregator"
	"metron/writers/signer"
	"metron/writers/tagger"

	"logger"
	"metron/eventwriter"
	"runtime"

	"github.com/cloudfoundry/dropsonde/metric_sender"
	"github.com/cloudfoundry/dropsonde/metricbatcher"
	"github.com/cloudfoundry/dropsonde/metrics"
	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"

	"metron/config"
)

var (
	logFilePath    = flag.String("logFile", "", "The agent log file, defaults to STDOUT")
	configFilePath = flag.String("config", "config/metron.json", "Location of the Metron config json file")
	debug          = flag.Bool("debug", false, "Debug logging")
)

func main() {
	// Metron is intended to be light-weight so we occupy only one core
	runtime.GOMAXPROCS(1)

	flag.Parse()
	config, err := config.ParseConfig(*configFilePath)
	if err != nil {
		panic(err)
	}

	log := logger.NewLogger(*debug, *logFilePath, "metron", config.Syslog)
	log.Info("Startup: Setting up the Metron agent")

	dopplerClientPool := initializeClientPool(config, log)

	dopplerForwarder := dopplerforwarder.New(dopplerClientPool, log)
	byteSigner := signer.New(config.SharedSecret, dopplerForwarder)
	marshaller := eventmarshaller.New(byteSigner, log)
	messageTagger := tagger.New(config.Deployment, config.Job, config.Index, marshaller)
	aggregator := messageaggregator.New(messageTagger, log)

	initializeMetrics(byteSigner, config, log)

	dropsondeUnmarshaller := eventunmarshaller.New(aggregator, log)
	dropsondeReader := networkreader.New(fmt.Sprintf("localhost:%d", config.DropsondeIncomingMessagesPort), "dropsondeAgentListener", dropsondeUnmarshaller, log)

	// TODO: remove next four lines when legacy support is removed (or extracted to injector)
	legacyMarshaller := eventmarshaller.New(byteSigner, log)
	legacyMessageTagger := tagger.New(config.Deployment, config.Job, config.Index, legacyMarshaller)
	legacyUnmarshaller := legacyunmarshaller.New(legacyMessageTagger, log)
	legacyReader := networkreader.New(fmt.Sprintf("localhost:%d", config.LegacyIncomingMessagesPort), "legacyAgentListener", legacyUnmarshaller, log)

	go legacyReader.Start()
	dropsondeReader.Start()
}

func initializeClientPool(config *config.Config, logger *gosteno.Logger) *clientpool.LoggregatorClientPool {
	adapter := storeAdapterProvider(config.EtcdUrls, config.EtcdMaxConcurrentRequests)
	err := adapter.Connect()
	if err != nil {
		logger.Errorf("Error connecting to ETCD: %v", err)
	}

	inZone := func(key string) bool {
		return strings.Index(key, metaZone) > 0 || strings.Index(key, legacyZone) > 0
	}

	dopplers := dopplerservice.NewFinder(adapter, inZone, logger)
	dopplers.Start()

	var legacyDopplers dopplerserver.Finder
	if config.PreferredProtocol == "udp" {
		legacyDopplers = dopplerservice.NewLegacyFinder(adapter, config.LoggregatorDropsondePort, inZone, logger)
		legacyDopplers.Start()
	}

	metaZone := path.Join(dopplerservice.META_ROOT, config.Zone)
	legacyZone := path.Join(dopplerservice.LEGACY_ROOT, config.Zone)

	clientPool := clientpool.NewDopplerClient(logger, config.PreferredProtocol, dopplers, legacyDopplers, inZone)
	return clientPool
}

func initializeMetrics(byteSigner *signer.Signer, config *config.Config, logger *gosteno.Logger) {
	metricsMarshaller := eventmarshaller.New(byteSigner, logger)
	metricsTagger := tagger.New(config.Deployment, config.Job, config.Index, metricsMarshaller)
	metricsAggregator := messageaggregator.New(metricsTagger, logger)

	eventWriter := eventwriter.New("MetronAgent", metricsAggregator)
	metricSender := metric_sender.NewMetricSender(eventWriter)
	metricBatcher := metricbatcher.New(metricSender, time.Duration(config.MetricBatchIntervalSeconds)*time.Second)
	metrics.Initialize(metricSender, metricBatcher)
}

func storeAdapterProvider(urls []string, concurrentRequests int) storeadapter.StoreAdapter {
	workPool, err := workpool.NewWorkPool(concurrentRequests)
	if err != nil {
		panic(err)
	}

	options := &etcdstoreadapter.ETCDOptions{
		ClusterUrls: urls,
	}
	etcdAdapter, err := etcdstoreadapter.New(options, workPool)
	if err != nil {
		panic(err)
	}
	return etcdAdapter
}

type metronHealthMonitor struct{}

func (*metronHealthMonitor) Ok() bool {
	return true
}
