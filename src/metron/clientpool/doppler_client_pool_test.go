package clientpool_test

import (
	"metron/clientpool"
	"metron/clientpool/fakes"

	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/loggregatorlib/loggregatorclient"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

//go:generate counterfeiter -o fakess/client.go ../../github.com/cloudfoundry/loggregatorlib/loggregatorclient Client
var _ = Describe("DopplerPool", func() {
	var (
		pool   *clientpool.DopplerPool
		logger *steno.Logger

		port int

		preferredServers map[string]loggregatorclient.Client
		allServers       map[string]loggregatorclient.Client

		preferredLegacy map[string]loggregatorclient.Client
		allLegacy       map[string]loggregatorclient.Client
	)

	BeforeEach(func() {
		logger = steno.NewLogger("TestLogger")

		port = 5000 + config.GinkgoConfig.ParallelNode*100

		preferredServers = map[string]loggregatorclient.Client{
			"a": newFakeClient("udp", "pahost:paport"),
			"b": newFakeClient("tls", "pbhost:pbport"),
		}
		allServers = map[string]loggregatorclient.Client{
			"a": newFakeClient("udp", "ahost:aport"),
			"b": newFakeClient("udp", "bhost:bport"),
		}

		preferredLegacy = map[string]loggregatorclient.Client{
			"a": newFakeClient("udp", "plahost:plaport"),
			"b": newFakeClient("tls", "plbhost:plbport"),
		}
		allLegacy = map[string]loggregatorclient.Client{
			"a": newFakeClient("udp", "lahost:laport"),
			"b": newFakeClient("udp", "lbhost:lbport"),
		}
	})

	JustBeforeEach(func() {
		pool = clientpool.NewDopplerPool(logger)
	})

	Context("non-Legacy servers", func() {
		Context("with preferred servers", func() {
			It("has only preferred servers", func() {
				pool.Set(allServers, preferredServers)
				Expect(urls(pool.Clients())).To(ConsistOf(values(preferredServers)))
			})
		})

		Context("with no preferred servers", func() {
			It("has only non-preferred servers", func() {
				pool.Set(allServers, nil)
				Expect(urls(pool.Clients())).To(ConsistOf(values(allServers)))
			})
		})

		Context("with Legacy Servers", func() {
			It("ignores them", func() {
				pool.Set(allServers, preferredServers)
				pool.SetLegacy(allLegacy, preferredLegacy)

				Expect(urls(pool.Clients())).To(ConsistOf(values(preferredServers)))
			})

			Context("with non-overlapping servers", func() {
				It("returns a mix of legacy and non-legacy", func() {
					allLegacy["c"] = newFakeClient("udp", "lchost:lcport")
					pool.Set(allServers, preferredServers)
					pool.SetLegacy(allLegacy, preferredLegacy)

					Expect(urls(pool.Clients())).To(ConsistOf(values(preferredServers)))
				})
			})
		})

		Context("only Legacy servers", func() {
			Context("with preferred servers", func() {
				It("has only preferred servers", func() {
					pool.SetLegacy(allLegacy, preferredLegacy)
					Expect(urls(pool.Clients())).To(ConsistOf(values(preferredLegacy)))
				})
			})

			Context("with no preferred servers", func() {
				It("has only non-preferred servers", func() {
					pool.SetLegacy(allLegacy, nil)
					Expect(urls(pool.Clients())).To(ConsistOf(values(allLegacy)))
				})
			})
		})
	})

	Describe("Clients", func() {
		Context("with no servers", func() {
			It("returns an empty list", func() {
				Expect(pool.Clients()).To(HaveLen(0))
			})
		})

		It("stops non-existant clients", func() {
			fakeClient := newFakeClient("udp", "host:port")
			s := map[string]loggregatorclient.Client{
				"a": fakeClient,
			}
			pool.Set(s, nil)
			Expect(fakeClient.StopCallCount()).To(Equal(0))
			pool.Set(nil, nil)
			Expect(fakeClient.StopCallCount()).To(Equal(1))
		})
	})

	Describe("RandomClient", func() {
		Context("with a non-empty client pool", func() {
			It("chooses a client with roughly uniform distribution", func() {
				s := map[string]loggregatorclient.Client{
					"1": newFakeClient("udp", "host:1"),
					"2": newFakeClient("udp", "host:2"),
					"3": newFakeClient("udp", "host:3"),
					"4": newFakeClient("udp", "host:4"),
					"5": newFakeClient("udp", "host:5"),
				}

				pool.Set(s, nil)
				counts := make(map[loggregatorclient.Client]int)
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

func urls(clients []loggregatorclient.Client) []string {
	result := make([]string, 0, len(clients))
	for _, c := range clients {
		result = append(result, c.Scheme()+"://"+c.Address())
	}
	return result
}

func values(m map[string]loggregatorclient.Client) []string {
	result := make([]string, 0, len(m))
	for _, c := range m {
		result = append(result, c.Scheme()+"://"+c.Address())
	}
	return result
}

func newFakeClient(proto, addr string) *fakes.FakeClient {
	c := fakes.FakeClient{}
	c.SchemeReturns(proto)
	c.AddressReturns(addr)
	return &c
}
