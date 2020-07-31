package pl.adkr.interview;

import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service which returns top (with maximum value) 1000 elements from endless data source of incoming elements.
 * <p>
 * P.S. Declarations of methods could be change accordingly without changing their names.
 */
@Slf4j
public class Top1000<E> {

    private static final Integer MAX_STORE_SIZE = 1000;

    private final Queue<E> store;

    private final AtomicBoolean isStoreReady = new AtomicBoolean(false);

    public Top1000(Comparator<E> comparator) {
        store = new PriorityQueue<>(comparator);
    }

    /**
     * Would be called in case of incoming element. One incoming element would be passed as argument
     */
    public synchronized void onEvent(E element) {
        log.debug("onEvent({})", element);
        store.add(element);
        if (store.size() > MAX_STORE_SIZE) {
            store.poll();
        }
        if (store.size() == MAX_STORE_SIZE) {
            if (!isStoreReady.get()) {
                log.debug("store size equals 1000. Releasing lock");
                isStoreReady.set(true);
                notifyAll();
            }
        } else {
            if (isStoreReady.get()) {
                log.debug("store size below 1000. Locking...");
                isStoreReady.set(false);
            }
        }
    }

    /**
     * Returns top (with maximum value) 1000 elements. Could be called anytime.
     */
    public synchronized Queue<E> getTop() {
        log.debug("getTop called");
        Queue<E> result = null;
        try {
            while (!isStoreReady.get()) {
                wait(2000);
                log.debug("Waiting for 1000 elements...");
            }
            result = new PriorityQueue<>(store);
        } catch (InterruptedException e) {
            log.error("Waiting interrupted", e);
        }
        log.debug("getTop returning...");
        return result;
    }
}

