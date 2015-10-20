package clientpool_test

import (
	"fmt"
	"math/rand"
	"time"

	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/loggregatorlib/clientpool"
	"github.com/cloudfoundry/loggregatorlib/loggregatorclient"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = BeforeSuite(func() {
	rand.Seed(int64(time.Now().Nanosecond()))
})

var _ = Describe("LoggregatorClientPool", func() {
	var (
		pool               *clientpool.LoggregatorClientPool
		logger             *steno.Logger
		fakeGetterInZone   *fakeAddressGetter
		fakeGetterAllZones *fakeAddressGetter
	)

	BeforeEach(func() {
		logger = steno.NewLogger("TestLogger")
		fakeGetterInZone = &fakeAddressGetter{}
		fakeGetterAllZones = &fakeAddressGetter{}
		pool = clientpool.NewLoggregatorClientPool(logger, 3456, fakeGetterInZone, fakeGetterAllZones)
	})

	Describe("ListClients", func() {
		Context("with empty address list", func() {
			It("returns an empty client list", func() {
				fakeGetterInZone.addresses = []string{}
				Expect(pool.ListClients()).To(HaveLen(0))
			})
		})

		Context("with a non-empty address list in-zone", func() {
			It("returns a client for every in-zone address", func() {
				fakeGetterInZone.addresses = []string{"127.0.0.1", "127.0.0.2"}
				Expect(pool.ListClients()).To(HaveLen(2))
			})

			It("does not include clients for out-of-zone servers", func() {
				fakeGetterInZone.addresses = []string{"127.0.0.1"}
				fakeGetterAllZones.addresses = []string{"127.0.0.2", "127.0.0.3"}
				Expect(pool.ListClients()).To(HaveLen(1))
			})
		})

		Context("with an empty in-zone address list and a non-empty all-zones address list", func() {
			It("returns a client for each all-zone address", func() {
				fakeGetterInZone.addresses = []string{}
				fakeGetterAllZones.addresses = []string{"127.0.0.1", "127.0.0.2"}
				Expect(pool.ListClients()).To(HaveLen(2))
			})
		})

		It("re-uses existing clients", func() {
			fakeGetterInZone.addresses = []string{"127.0.0.1"}
			client1 := pool.ListClients()[0]
			client2 := pool.ListClients()[0]
			Expect(client1).To(Equal(client2))
		})
	})

	Describe("RandomClient", func() {
		Context("with a non-empty client pool", func() {
			It("chooses a client with roughly uniform distribution", func() {
				for i := 0; i < 5; i++ {
					fakeGetterInZone.addresses = append(fakeGetterInZone.addresses, fmt.Sprintf("127.0.0.%d", i))
				}

				counts := make(map[loggregatorclient.LoggregatorClient]int)
				for i := 0; i < 100000; i++ {
					pick, _ := pool.RandomClient()
					counts[pick]++
				}

				for _, count := range counts {
					Expect(count).To(BeNumerically("~", 20000, 500))
				}
			})
		})

		Context("with an empty client pool", func() {
			It("returns an error", func() {
				_, err := pool.RandomClient()
				Expect(err).To(Equal(clientpool.ErrorEmptyClientPool))
			})
		})
	})

})

type fakeAddressGetter struct {
	addresses []string
}

func (getter *fakeAddressGetter) GetAddresses() []string {
	return getter.addresses
}
