package org.angnysa.dispatcher;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

@RequiredArgsConstructor
public abstract class StatisticMonitor<G, M> implements Monitor<M> {
    protected static class Count {
        private final AtomicLong received = new AtomicLong();
        private final AtomicLong successes = new AtomicLong();
        private final AtomicLong errors = new AtomicLong();

        public long getReceived() {
            return received.get();
        }

        public long getSuccesses() {
            return successes.get();
        }

        public long getErrors() {
            return errors.get();
        }
    }

    @NonNull @Getter private final Function<M, G> groupGetter;
    @Getter private final long period;
    @NonNull @Getter private final TimeUnit periodUnit;
    @Getter private ConcurrentMap<G, Count> periodCountPerGroups = new ConcurrentHashMap<>();
    @Getter private Count globalPeriodCount = new Count();
    @Getter private Count totalCount = new Count();
    private ReadWriteLock periodSwitchLock = new ReentrantReadWriteLock(true);

    private volatile long nextPeriod = 0;

    @Override
    public void onMessageStarting(M message) {
        checkPeriod();
        try {
            periodSwitchLock.readLock().lock();
            periodCountPerGroups.computeIfAbsent(groupGetter.apply(message), (g) -> new Count()).received.incrementAndGet();
            globalPeriodCount.received.incrementAndGet();
            totalCount.received.incrementAndGet();
        } finally {
            periodSwitchLock.readLock().unlock();
        }
    }

    @Override
    public void onMessageComplete(M message) {
        checkPeriod();
        try {
            periodSwitchLock.readLock().lock();
            periodCountPerGroups.computeIfAbsent(groupGetter.apply(message), (g) -> new Count()).successes.incrementAndGet();
            globalPeriodCount.successes.incrementAndGet();
            totalCount.successes.incrementAndGet();
        } finally {
            periodSwitchLock.readLock().unlock();
        }
    }

    @Override
    public void onMessageFailure(M message, Exception e) {
        checkPeriod();
        try {
            periodSwitchLock.readLock().lock();
            periodCountPerGroups.computeIfAbsent(groupGetter.apply(message), (g) -> new Count()).errors.incrementAndGet();
            globalPeriodCount.errors.incrementAndGet();
            totalCount.errors.incrementAndGet();
        } finally {
            periodSwitchLock.readLock().unlock();
        }
    }

    private void checkPeriod() {
        if (System.nanoTime() > nextPeriod) {
            ConcurrentMap<G, Count> groupStat = null;
            Count globalStat = null;
            Count total = null;

            try {
                periodSwitchLock.writeLock().lock();
                if (System.nanoTime() >= nextPeriod) {
                    nextPeriod = System.nanoTime()+periodUnit.toNanos(period);

                    groupStat = periodCountPerGroups;
                    globalStat = globalPeriodCount;
                    total = new Count();
                    total.received.set(totalCount.received.get());
                    total.successes.set(totalCount.successes.get());
                    total.errors.set(totalCount.errors.get());

                    periodCountPerGroups = new ConcurrentHashMap<>();
                    globalPeriodCount = new Count();
                }
            } finally {
                periodSwitchLock.writeLock().unlock();
            }

            if (groupStat != null) {
                onPeriodDone(groupStat, globalStat, total);
            }
        }
    }

    protected abstract void onPeriodDone(ConcurrentMap<G, Count> groupStat, Count globalStat, Count total);
}
