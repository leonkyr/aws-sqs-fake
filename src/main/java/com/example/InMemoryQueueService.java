package com.example;

import com.example.exceptions.QueueDoesNotExistException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class InMemoryQueueService extends BaseLocalQueueService {

    private static final int DEFAULT_THREAD_COUNT = 5;

    private final Map<String, List<DefaultMessage>> queues =
            new ConcurrentHashMap<>();

    // Maybe not the optimal struct for queue, especially if the list is there -> O(N)
    private final Map<String, List<DefaultMessage>> queuesWithPolledMessages =
            new ConcurrentHashMap<>();

    // constructor for DI Container (f.e. Spring)
    public InMemoryQueueService(
            HashCalculator hashCalculator,
            ScheduledExecutorService scheduledExecutorService,
            Logger logger) {
        super(hashCalculator, scheduledExecutorService, logger);
    }

    public InMemoryQueueService(){
        this(new MD5HashCalculator(), Executors.newScheduledThreadPool(DEFAULT_THREAD_COUNT), new SimpleConsoleLogger());
    }

    @Override
    public void doPush(String queueUrl, DefaultMessage internalMessage) {

        queues
                .get(queueUrl)
                .add(internalMessage);
    }

    @Override
    protected Message doPull(String queueUrl, Integer visibilityTimeout)
            throws InterruptedException, IOException {

        List<DefaultMessage> queue = queues.get(queueUrl);

        if (queue.isEmpty()) {
            getLogger().w("There are not messages in the queue. leaving");
            // I could use NullMessage class also
            return null;
        }

        final List<DefaultMessage> queueWithPolled = queuesWithPolledMessages.get(queueUrl);

        final DefaultMessage internalMessage = queue.remove(0);

        setVisibilityTimeoutToMessage(visibilityTimeout, internalMessage);
        final String receiptHandle = setReceiptHandleForMessage(internalMessage);
        queueWithPolled.add(internalMessage);

        scheduleMessageDeleteVerification(queue, queueWithPolled, internalMessage, receiptHandle);

        getLogger().w("PULLED internalMessage = " + internalMessage);
        return internalMessage;
    }

    private void scheduleMessageDeleteVerification(
            List<DefaultMessage> queue, List<DefaultMessage> queueWithPolled,
            DefaultMessage internalMessage, String receiptHandle) {

        getScheduledExecutorService()
            .schedule(
                    () -> {
                        DefaultMessage msg = queueWithPolled
                                .stream()
                                .filter(m -> m.getReceiptHandle().equals(receiptHandle))
                                .findFirst()
                                .orElseGet(() -> null);

                        // message was NOT deleted
                        if (msg != null) {
                            // let me put it back
                            queueWithPolled.remove(internalMessage);
                            //  at the head
                            queue.add(0, internalMessage);
                        }
                    },
                    internalMessage.getVisibilityTimeout(), TimeUnit.SECONDS);
    }

    @Override
    protected void doDelete(String queueUrl, String receiptHandle) {

        queuesWithPolledMessages.get(queueUrl)
                .removeIf(msg -> msg.getReceiptHandle().equals(receiptHandle));
    }

    @Override
    protected void validateQueue(String queueUrl) {
        if (queueUrl == null ||
                queueUrl.isEmpty() ||
                !queues.containsKey(queueUrl) ||
                !queuesWithPolledMessages.containsKey(queueUrl)) {
            throw new QueueDoesNotExistException("Queue " + queueUrl + " does not exist.");
        }
    }

    @Override
    protected String doCreateQueue(String queueName) {
        queues.computeIfAbsent(queueName, key -> new CopyOnWriteArrayList<>());
        queuesWithPolledMessages.computeIfAbsent(queueName, key -> new CopyOnWriteArrayList<>());

        return queueName;
    }

    @Override
    protected void doDeleteQueue(String queueUrl) {
        queues.remove(queueUrl);
        queuesWithPolledMessages.remove(queueUrl);
    }
}