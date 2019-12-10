package org.angnysa.dispatcher;

import lombok.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>This class receives and handle the processing of a stream of partially parallelizable messages.</p>
 *
 * <p>Each message must have the following constraints</p>
 * <ul>
 *     <li>Each message <b>MUST</b> be part of a group.</li>
 *     <li>Groups will be known at run-time only.</li>
 *     <li>Messages of a same group <b>MUST</b> be processed in the order they were {@link #submit(Object) submitted}.</li>
 *     <li>Messages of different groups <b>MAY</b> be processed in parallel.</li>
 * </ul>
 *
 * TODO expire and remove groups
 *
 * @param <M> The message class
 * @param <G> The group class
 */
@RequiredArgsConstructor
@AllArgsConstructor
public class Dispatcher<M, G> {

    /**
     * Internal data associated with a group
     */
    private class Group {
        /**
         * Queue of messages of the same group
         */
        private final BlockingQueue<M> queue;

        /**
         * Marker indicating that a thread is (or not) processing the messages in the {@link #queue}.
         */
        private final AtomicBoolean held = new AtomicBoolean(false);

        private Group() {
            int limit = getMaxMessagePerGroup();
            if (limit == 0) {
                this.queue = new SynchronousQueue<>();
            } else {
                this.queue = new LinkedBlockingQueue<>(limit);
            }
        }
    }

    /**
     * Timeout for {@link BlockingQueue#poll(long, TimeUnit)}.
     *
     * @see #processGroup(Group)
     */
    @Getter private volatile long groupPollTimeout = 0;

    /**
     * Unit for {@link #groupPollTimeout}
     */
    @NonNull @Getter private volatile TimeUnit groupPollTimeoutUnit = TimeUnit.MILLISECONDS;

    /**
     * Timeout for {@link BlockingQueue#offer(Object, long, TimeUnit)}.
     *
     * @see #submit(Object)
     */
    @Getter private volatile long groupOfferTimeout = 1;

    /**
     * Unit for {@link #groupOfferTimeout}
     */
    @NonNull @Getter private volatile TimeUnit groupOfferTimeoutUnit = TimeUnit.MILLISECONDS;

    /**
     * Maximum number of message allowed to be queued in {@link Group#queue}
     */
    @Getter private volatile int maxMessagePerGroup = 0;

    /**
     * Maximum number of messages allowed to be queued in all groups.
     */
    @Getter private volatile int maxTotalMessage = Integer.MAX_VALUE;

    /**
     * Map of all known groups.
     */
    private final ConcurrentMap<G, Group> groupsMap = new ConcurrentHashMap<>();

    /**
     * Thread pool for processing the groups
     */
    @NonNull @Getter private final ThreadPoolExecutor executorService;

    /**
     * User-provided callback to obtain the group from a message.
     */
    @NonNull private final Function<M, G> groupGetter;

    /**
     * User-provided callback for actually doing something with the message.
     */
    @NonNull private final Consumer<M> messageProcessor;

    /**
     * Implementation of the global maximum number of message limit.
     */
    @NonNull private Semaphore totalMessageSemaphore = new Semaphore(Integer.MAX_VALUE, true);

    /**
     * Monitoring (debug, statistic, etc.) object.
     */
    @Getter @Setter private Monitor<M> monitor;

    /**
     * Same as {@link #Dispatcher(Function, Consumer)}, and allows a Monitor.
     *
     * @param groupGetter The callback for obtaining the group.
     * @param messageProcessor The callback for processing a message.
     * @param monitor The monitor object
     */
    public Dispatcher(Function<M, G> groupGetter, Consumer<M> messageProcessor, Monitor<M> monitor) {
        this(groupGetter, messageProcessor);
        setMonitor(monitor);
    }


    /**
     * Calls {@link #Dispatcher(int, int, long, TimeUnit, Function, Consumer) Dispatcher(0, Integer.MAX_VALUE, 30L, TimeUnit.SECOND, groupGetter, messageProcessor)}
     *
     * @param groupGetter The callback for obtaining the group.
     * @param messageProcessor The callback for processing a message.
     */
    public Dispatcher(Function<M, G> groupGetter, Consumer<M> messageProcessor) {
        this(0, Integer.MAX_VALUE, 30L, TimeUnit.SECONDS, groupGetter, messageProcessor);
    }

    /**
     * Same as {@link #Dispatcher(int, int, long, TimeUnit, Function, Consumer)}, and allows a Monitor.
     *
     * @param corePoolSize Parameter for {@link ThreadPoolExecutor}.
     * @param maximumPoolSize Parameter for {@link ThreadPoolExecutor}.
     * @param keepAliveTime Parameter for {@link ThreadPoolExecutor}.
     * @param keepAliveUnit Parameter for {@link ThreadPoolExecutor}.
     * @param groupGetter The callback for obtaining the group.
     * @param messageProcessor The callback for processing a message.
     * @param monitor The monitor object
     *
     */
    public Dispatcher(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit keepAliveUnit, Function<M, G> groupGetter, Consumer<M> messageProcessor, Monitor<M> monitor) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, keepAliveUnit, groupGetter, messageProcessor);
        setMonitor(monitor);
    }


    /**
     * Calls #Dispatcher(ThreadPoolExecutor, Function, Consumer) with a thread pool set up to use a {@link SynchronousQueue} and {@link ThreadPoolExecutor.CallerRunsPolicy}
     *
     * @param corePoolSize Parameter for {@link ThreadPoolExecutor}.
     * @param maximumPoolSize Parameter for {@link ThreadPoolExecutor}.
     * @param keepAliveTime Parameter for {@link ThreadPoolExecutor}.
     * @param keepAliveUnit Parameter for {@link ThreadPoolExecutor}.
     * @param groupGetter The callback for obtaining the group.
     * @param messageProcessor The callback for processing a message.
     */
    public Dispatcher(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit keepAliveUnit, Function<M, G> groupGetter, Consumer<M> messageProcessor) {
        this(
                new ThreadPoolExecutor(
                        corePoolSize,
                        maximumPoolSize,
                        keepAliveTime,
                        keepAliveUnit,
                        new SynchronousQueue<>(true),
                        new ThreadPoolExecutor.CallerRunsPolicy()),
                groupGetter, messageProcessor);
    }

    /**
     * @see #groupOfferTimeout
     * @see #groupOfferTimeoutUnit
     *
     * @param groupOfferTimeout
     * @param groupOfferTimeoutUnit
     */
    public void setGroupOfferTimeout(long groupOfferTimeout, TimeUnit groupOfferTimeoutUnit) {
        this.groupOfferTimeout = groupOfferTimeout;
        this.groupOfferTimeoutUnit = groupOfferTimeoutUnit;
    }

    /**
     * @see #groupPollTimeout
     * @see #groupPollTimeoutUnit
     *
     * @param groupPollTimeout
     * @param groupPollTimeoutUnit
     */
    public void setGroupPollTimeout(long groupPollTimeout, TimeUnit groupPollTimeoutUnit) {
        this.groupPollTimeout = groupPollTimeout;
        this.groupPollTimeoutUnit = groupPollTimeoutUnit;
    }

    /**
     * @see #maxMessagePerGroup
     *
     * @param maxMessagePerGroup
     */
    public void setMaxMessagePerGroup(int maxMessagePerGroup) {
        // TODO change the queues in existing groups
        this.maxMessagePerGroup = maxMessagePerGroup;
    }

    /**
     * @see #maxTotalMessage
     *
     * @param maxTotalMessage
     */
    public void setMaxTotalMessage(int maxTotalMessage) {
        int delta = maxTotalMessage - this.maxTotalMessage;

        if (delta < 0) {
            totalMessageSemaphore.acquireUninterruptibly(-delta);
        } else if (delta > 0) {
            totalMessageSemaphore.release(delta);
        }
        this.maxTotalMessage = maxTotalMessage;
    }

    /**
     * Submit a new message for execution, blocking until submitted or interrupted.
     *
     * @param message
     * @throws InterruptedException
     * @throws RejectedExecutionException If the thread pool rejected the task
     */
    public void submit(M message) throws InterruptedException, RejectedExecutionException {

        G groupKey = groupGetter.apply(message);
        Group group = groupsMap.computeIfAbsent(groupKey, (g) -> new Group());

        totalMessageSemaphore.acquire();

        try {
            do {
                if (!group.held.get()) {
                    getExecutorService().submit(() -> processGroup(group));
                }
            } while (!group.queue.offer(message, getGroupOfferTimeout(), getGroupOfferTimeoutUnit()));
        } catch (Throwable t) {
            totalMessageSemaphore.release();
            throw t;
        }
    }

    /**
     * Stop accepting new messages, finish any remaining message in the groups, and shuts down the thread pool.
     * {@link ExecutorService#awaitTermination(long, TimeUnit) getExecutorService().awaitTermination()} can be called to wait for complete shutdown.
     */
    public void shutdown() {
        getExecutorService().shutdown();

    }

    /**
     * Processes all messages in a group, wait for the duration defined by
     * {@link #getGroupPollTimeout()}/{@link #getGroupPollTimeoutUnit()}, and returns to the pool.
     *
     * Given by {@link #submit(Object)} to the thread pool when a message is added to a group that no thread is
     * processing.
     *
     * @param group The group where one message was just added.
     */
    @SneakyThrows(InterruptedException.class)
    private void processGroup(Group group) {
        if (group.held.compareAndSet(false, true)) {
            try {
                M message;
                while ((message = group.queue.poll(getGroupPollTimeout(), getGroupPollTimeoutUnit())
                        ) != null) {
                    if (monitor != null) {
                        monitor.onMessageStarting(message);
                    }

                    Exception ex = null;
                    try {
                        messageProcessor.accept(message);
                    } catch (Exception e) {
                        ex = e;
                    } finally {
                        totalMessageSemaphore.release();
                    }

                    if (monitor != null) {
                        if (ex != null) {
                            monitor.onMessageFailure(message, ex);
                        } else {
                            monitor.onMessageComplete(message);
                        }
                    }
                }
            } finally {
                group.held.set(false);
            }
        }
    }
}
