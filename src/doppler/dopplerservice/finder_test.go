package dopplerservice_test

import (
	"errors"

	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/loggregatorlib/loggertesthelper"
	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"

	"doppler/dopplerservice"
	"doppler/dopplerservice/fakes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

//go:generate counterfeiter -o fakes/fakestoreadapter.go ../../github.com/cloudfoundry/storeadapter StoreAdapter
var _ = Describe("Finder", func() {
	var finder dopplerservice.Finder

	Describe("Running", func() {
		var (
			order       chan string
			fakeAdapter *fakes.FakeStoreAdapter
			errChan     chan error
			stopChan    chan bool
		)

		BeforeEach(func() {
			order = make(chan string, 2)
			stopChan = make(chan bool)
			errChan = make(chan error, 1)

			fakeAdapter = &fakes.FakeStoreAdapter{}
			fakeAdapter.WatchReturns(nil, stopChan, errChan)
		})

		JustBeforeEach(func() {
			finder = dopplerservice.NewFinder(fakeAdapter, loggertesthelper.Logger())
			finder.Start()
		})

		AfterEach(func() {
			finder.Stop()
		})

		Context("Start", func() {
			BeforeEach(func() {
				fakeAdapter.WatchStub = func(key string) (events <-chan storeadapter.WatchEvent, stop chan<- bool, errors <-chan error) {
					Expect(key).To(Equal(dopplerservice.META_ROOT))
					order <- "watch"
					return nil, stopChan, errChan
				}
				fakeAdapter.ListRecursivelyStub = func(key string) (storeadapter.StoreNode, error) {
					Expect(key).To(Equal(dopplerservice.META_ROOT))
					order <- "finder"
					return storeadapter.StoreNode{}, nil
				}
			})

			It("watches and finders nodes recursively", func() {
				Eventually(order).Should(Receive(Equal("watch")))
				Eventually(order).Should(Receive(Equal("finder")))

				By("an error occurs")

				Expect(errChan).To(BeSent(errors.New("an error")))

				Eventually(order).Should(Receive(Equal("watch")))
				Eventually(order).Should(Receive(Equal("finder")))
			})

		})

		It("Stop", func() {
			finder.Stop()
			Eventually(stopChan).Should(BeClosed())
		})
	})

	Context("with a real etcd", func() {
		var (
			storeAdapter storeadapter.StoreAdapter
			node         storeadapter.StoreNode
			updateNode   storeadapter.StoreNode
		)

		BeforeEach(func() {
			workPool, err := workpool.NewWorkPool(10)
			Expect(err).NotTo(HaveOccurred())
			options := &etcdstoreadapter.ETCDOptions{
				ClusterUrls: etcdRunner.NodeURLS(),
			}
			storeAdapter, err = etcdstoreadapter.New(options, workPool)
			Expect(err).NotTo(HaveOccurred())

			err = storeAdapter.Connect()
			Expect(err).NotTo(HaveOccurred())

			node = storeadapter.StoreNode{
				Key:   dopplerservice.LEGACY_ROOT + "/z1/loggregator_z1/0",
				Value: []byte("10.0.0.1"),
			}
		})

		AfterEach(func() {
			finder.Stop()
		})

		testUpdateFinder := func(updatedAddress ...string) func() {
			return func() {
				Context("when a service exists", func() {
					BeforeEach(func() {
						err := storeAdapter.Create(node)
						Expect(err).NotTo(HaveOccurred())

						finder.Start()
						Eventually(finder.Addresses).ShouldNot(BeZero())
					})

					It("updates the service", func() {
						err := storeAdapter.Update(updateNode)
						Expect(err).NotTo(HaveOccurred())

						Eventually(finder.Addresses).Should(ConsistOf(updatedAddress))
					})
				})
			}
		}

		testFinder := func(expectedAddress ...string) func() {
			return func() {
				Context("when a service is created", func() {
					Context("before dopplerservice begins", func() {
						It("finds server addresses that were created before it started", func() {
							Expect(finder.Addresses()).To(HaveLen(0))

							storeAdapter.Create(node)
							finder.Start()

							Eventually(finder.Addresses).Should(ConsistOf(expectedAddress))
						})
					})

					Context("after dopplerservice begins", func() {
						It("adds servers that appear later", func() {
							finder.Start()
							Consistently(finder.Addresses, 1).Should(BeEmpty())

							storeAdapter.Create(node)

							Eventually(finder.Addresses).Should(ConsistOf(expectedAddress))
						})
					})
				})

				Context("when services exists", func() {
					BeforeEach(func() {
						err := storeAdapter.Create(node)
						Expect(err).NotTo(HaveOccurred())

						finder.Start()
						Eventually(finder.Addresses).ShouldNot(HaveLen(0))
					})

					It("removes the service", func() {
						err := storeAdapter.Delete(node.Key)
						Expect(err).NotTo(HaveOccurred())
						Eventually(finder.Addresses).Should(BeEmpty())
					})

					It("only finds nodes for the doppler server type", func() {
						router := storeadapter.StoreNode{
							Key:   "/healthstatus/router/z1/router_z1",
							Value: []byte("10.99.99.99"),
						}
						storeAdapter.Create(router)

						Consistently(finder.Addresses).Should(ConsistOf(expectedAddress))
					})

					Context("when ETCD is not running", func() {
						JustBeforeEach(func() {
							etcdRunner.Stop()
						})

						AfterEach(func() {
							etcdRunner.Start()
						})

						It("continues to run if the store times out", func() {
							Consistently(finder.Addresses).ShouldNot(HaveLen(0))
						})
					})
				})
			}
		}

		Context("LegacyFinder", func() {
			BeforeEach(func() {
				finder = dopplerservice.NewLegacyFinder(storeAdapter, 1234, loggertesthelper.Logger())
				node = storeadapter.StoreNode{
					Key:   dopplerservice.LEGACY_ROOT + "/z1/loggregator_z1/0",
					Value: []byte("10.0.0.1"),
				}

				updateNode = node
				updateNode.Value = []byte("10.0.0.2")
			})

			Context("Basics", testFinder("10.0.0.1:1234"))
			Context("Update", testUpdateFinder("10.0.0.2:1234"))
		})

		Context("Finder", func() {
			BeforeEach(func() {
				finder = dopplerservice.NewFinder(storeAdapter, loggertesthelper.Logger())
			})

			Context("with a single endpoint", func() {
				BeforeEach(func() {
					node = storeadapter.StoreNode{
						Key:   dopplerservice.META_ROOT + "/z1/loggregator_z1/0",
						Value: []byte(`{"version": 1, "endpoints":["udp://10.0.0.1:1234"]}`),
					}

					updateNode = node
					updateNode.Value = []byte(`{"version": 1, "endpoints":["udp://10.0.0.2:1234"]}`)
				})

				Context("Basics", testFinder("udp://10.0.0.1:1234"))
				Context("Update", testUpdateFinder("udp://10.0.0.2:1234"))
			})

			Context("with multiple endpoints", func() {
				BeforeEach(func() {
					node = storeadapter.StoreNode{
						Key:   dopplerservice.META_ROOT + "/z1/loggregator_z1/0",
						Value: []byte(`{"version": 1, "endpoints":["udp://10.0.0.1:1234", "tls://10.0.0.2:4567"]}`),
					}

					updateNode = node
					updateNode.Value = []byte(`{"version": 1, "endpoints":["udp://10.0.0.2:1235", "tls://10.0.0.3:4568"]}`)
				})

				Context("Basics", testFinder("udp://10.0.0.1:1234", "tls://10.0.0.2:4567"))
				Context("Update", testUpdateFinder("udp://10.0.0.2:1235", "tls://10.0.0.3:4568"))
			})
		})
	})
})
