package pl.adkr.interview

import spock.lang.Specification
import spock.lang.Subject

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.stream.IntStream

class Top1000Test extends Specification {

    @Subject
    def top1000

    def setup() {
        top1000 = new Top1000<Integer>(new Comparator<Integer>() {
            @Override
            int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        })
    }

    def 'should keep only top values ascending'() {
        when:
        fillStoreWithData(1, 2000)

        then:
        for (int i = 0; i < 1000; i++) {
            top1000.store.poll() == i + 1000
        }
    }

    def 'should allow duplicates'() {
        when:
        IntStream.of(1, 2, 3, 1, 2, 3)
                .forEach({ i ->
                    top1000.onEvent(i)
                })

        then:
        top1000.store.poll() == 1
        top1000.store.poll() == 1
        top1000.store.poll() == 2
        top1000.store.poll() == 2
        top1000.store.poll() == 3
        top1000.store.poll() == 3
    }

    def 'should not return top when values count less than 1000'() {
        given:
        fillStoreWithData(1, 1000)
        def executor = Executors.newSingleThreadExecutor()

        def expectedSize = 0

        when:
        executor.execute(new Runnable() {
            @Override
            void run() {
                expectedSize = top1000.getTop().size()
            }
        })
        executor.awaitTermination(5, TimeUnit.SECONDS)

        then:
        expectedSize == 0
    }

    def 'should return top when values count equals 1000'() {
        given:
        fillStoreWithData(1, 1001)

        def expectedSize = 1000
        def returnedTop = null

        when:
        def executor = Executors.newSingleThreadExecutor()
        executor.execute(new Runnable() {
            @Override
            void run() {
                returnedTop = top1000.getTop()
            }
        })
        executor.awaitTermination(5, TimeUnit.SECONDS)
        executor.shutdownNow()

        then:
        expectedSize == returnedTop.size()
    }

    def 'should work with two threads'() {
        given:
        def waitForData999 = new CountDownLatch(1)
        def waitForOtherThreadToTryGetTop999Items = new CountDownLatch(1)
        def waitForOtherThreadToTryGetTop1000Items = new CountDownLatch(1)
        def globalLatch = new CountDownLatch(1)

        def producer = new Thread(new Runnable() {
            @Override
            void run() {
                fillStoreWithData(1, 1000)
                println 'waiting...'
                waitForData999.countDown()
                waitForOtherThreadToTryGetTop999Items.await(2, TimeUnit.SECONDS)
                println 'running again...'

                top1000.onEvent(1000)
                waitForOtherThreadToTryGetTop1000Items.countDown()
            }
        })

        Thread consumer = new Thread(new Runnable() {
            @Override
            void run() {
                waitForData999.await()
                def resultSize = top1000.getTop().size()
                waitForOtherThreadToTryGetTop1000Items.await()
                assert resultSize == 1000
                globalLatch.countDown()
            }
        })

        when:
        producer.start()
        consumer.start()

        then:
        globalLatch.await()
    }

    private fillStoreWithData(Integer from, Integer to) {
        IntStream.range(from, to)
                .forEach({ i ->
                    top1000.onEvent(i)
                })
    }
}
