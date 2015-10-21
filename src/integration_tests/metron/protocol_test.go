package integration_test

import (
	"bytes"
	"io/ioutil"
	"net"
	"os/exec"
	"strconv"
	"time"

	dopplerconfig "doppler/config"
	"doppler/dopplerservice"

	"github.com/cloudfoundry/gosteno"
	. "github.com/onsi/ginkgo"
	ginkgoconfig "github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
)

var _ = Describe("Protocol", func() {
	var (
		process           ifrit.Process
		preferredProtocol string
		port              int
		dropsondePort     string
	)

	start := func(protocol string) ifrit.Process {
		cfgFile, err := ioutil.TempFile(tmpdir, "metron")
		Expect(err).NotTo(HaveOccurred())
		_, err = cfgFile.WriteString(`
{
    "Index": 42,
    "Job": "test-component",
    "LegacyIncomingMessagesPort": ` + strconv.Itoa(port) + `,
    "DropsondeIncomingMessagesPort":` + dropsondePort + `,
    "SharedSecret": "shared_secret",
    "EtcdUrls"    : ["` + etcdRunner.NodeURLS()[0] + `"],
    "EtcdMaxConcurrentRequests": 1,
    "Zone": "z1",
    "Deployment": "deployment-name",
    "LoggregatorDropsondePort": 3457,
    "PreferredProtocol": "` + protocol + `"
}`)
		Expect(err).NotTo(HaveOccurred())
		cfgFile.Close()

		runner := ginkgomon.New(ginkgomon.Config{
			Name:          "metron",
			AnsiColorCode: "97m",
			StartCheck:    "metron started",
			Command: exec.Command(
				pathToMetronExecutable,
				"--config", cfgFile.Name(),
				"--debug",
			),
		})

		return ginkgomon.Invoke(runner)
	}

	JustBeforeEach(func() {
		port = 51000 + ginkgoconfig.GinkgoConfig.ParallelNode*100
		dropsondePort = strconv.Itoa(port + 1)
		start(preferredProtocol)
	})

	AfterEach(func() {
		ginkgomon.Interrupt(process, time.Second)
	})

	Context("with UDP preferred", func() {

		Context("Legacy Doppler", func() {
			var fakeDoppler *FakeDoppler
			var stopTheWorld chan struct{}

			BeforeEach(func() {
				stopTheWorld = make(chan struct{})

				conn := eventuallyListensForUDP("localhost:3457")
				fakeDoppler = &FakeDoppler{
					packetConn:   conn,
					stopTheWorld: stopTheWorld,
				}

				dopplerConfig := &dopplerconfig.Config{
					Index:             0,
					JobName:           "job",
					Zone:              "z9",
					TLSListenerConfig: dopplerconfig.TLSListenerConfig{Port: uint32(port + 5)},
				}
				dopplerservice.AnnounceLegacy("127.0.0.1", time.Minute, dopplerConfig, etcdAdapter, gosteno.NewLogger("test"))
			})

			AfterEach(func() {
				fakeDoppler.Close()
				close(stopTheWorld)
			})

			It("forwards hmac signed messages to a healthy doppler server", func() {
				originalMessage := basicValueMessage()
				expectedMessage := sign(originalMessage)

				receivedByDoppler := fakeDoppler.ReadIncomingMessages(expectedMessage.signature)

				metronConn, _ := net.Dial("udp4", "127.0.0.1:"+dropsondePort)
				metronInput := &MetronInput{
					metronConn:   metronConn,
					stopTheWorld: stopTheWorld,
				}
				go metronInput.WriteToMetron(originalMessage)

				Eventually(func() bool {
					var msg signedMessage
					Eventually(receivedByDoppler).Should(Receive(&msg))
					return bytes.Equal(msg.signature, expectedMessage.signature) && bytes.Equal(msg.message, expectedMessage.message)
				}).Should(BeTrue())
			}, 2)
		})

		Context("Doppler with UDP", func() {})
		Context("Doppler with TLS", func() {})
	})

	Context("with TLS preferred", func() {
		Context("Legacy Doppler", func() {})
		Context("Doppler with UDP", func() {})
		Context("Doppler with TLS", func() {})
	})
})
