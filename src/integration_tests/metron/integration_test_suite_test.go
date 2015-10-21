package integration_test

import (
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	"github.com/pivotal-golang/localip"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"

	"github.com/onsi/ginkgo/config"
	"github.com/onsi/gomega/gexec"
)

func TestIntegrationTest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "IntegrationTest Suite")
}

var tmpdir string
var pathToMetronExecutable string
var metronSession *gexec.Session
var etcdRunner *etcdstorerunner.ETCDClusterRunner
var etcdAdapter storeadapter.StoreAdapter
var etcdPort int
var localIPAddress string

var _ = BeforeSuite(func() {
	var err error
	tmpdir, err = ioutil.TempDir("", "metronintg")
	Expect(err).ShouldNot(HaveOccurred())

	pathToMetronExecutable, err = gexec.Build("metron")
	Expect(err).ShouldNot(HaveOccurred())

	localIPAddress, _ = localip.LocalIP()

	etcdPort = 5800 + (config.GinkgoConfig.ParallelNode-1)*10
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1, nil)
	etcdRunner.Start()
	etcdAdapter = etcdRunner.Adapter(nil)

	metronSession, err = startMetron()
	Expect(err).ShouldNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	stopMetron()
	gexec.CleanupBuildArtifacts()

	etcdAdapter.Disconnect()
	etcdRunner.Stop()
	os.RemoveAll(tmpdir)
})

var _ = BeforeEach(func() {
	etcdRunner.Reset()
})

func startMetron() (*gexec.Session, error) {
	command := exec.Command(pathToMetronExecutable, "--config=fixtures/metron.json", "--debug")
	return gexec.Start(command, gexec.NewPrefixedWriter("[o][metron]", GinkgoWriter), gexec.NewPrefixedWriter("[e][metron]", GinkgoWriter))
}

func stopMetron() {
	metronSession.Kill().Wait()
}
