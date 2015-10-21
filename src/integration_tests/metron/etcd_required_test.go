package integration_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("Etcd dependency", func() {
	Context("when etcd is down", func() {
		BeforeEach(func() {
			stopMetron()
			etcdRunner.Stop()
		})

		AfterEach(func() {
			etcdRunner.Start()
			startMetron()
		})

		It("fails to start", func() {
			session, err := startMetron()
			Expect(err).NotTo(HaveOccurred())
			Eventually(session).Should(gexec.Exit(-1))
			Expect(session.Out).To(gbytes.Say("Error while initializing client pool"))
		})
	})
})
