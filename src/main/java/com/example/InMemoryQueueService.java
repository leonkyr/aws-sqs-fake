package com.example;

import com.example.exceptions.QueueDoesNotExistException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class InMemoryQueueService extends BaseLocalQueueService {
    private static final Integer DEFAULT_VISIBILITY_TIMEOUT = 30;
    private static final String NOT_SET_RECEIPT_HANDLE = "";
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
    public void push(String queueUrl, String messageBody)
            throws InterruptedException, IOException {

        getLogger().w("INMEM push -> queueName = [" + queueUrl + "], messageBoy = [" + messageBody + "]");

        validateQueues(queueUrl);

        final DefaultMessage internalMessage =
                DefaultMessage.create(
                        generateMessageId(),
                        NOT_SET_RECEIPT_HANDLE,
                        messageBody,
                        getHashCalculator().calculate(messageBody));

        getLogger().w("PUSHED internalMessage = " + internalMessage);

        queues
                .get(queueUrl)
                .add(internalMessage);
    }

    @Override
    public Message pull(String queueUrl)
            throws InterruptedException, IOException {

        return pull(queueUrl, DEFAULT_VISIBILITY_TIMEOUT);
    }

    @Override
    public Message pull(String queueUrl, Integer visibilityTimeout)
            throws InterruptedException, IOException {
        // we don't care that a consumer make consume multiple message at the same time

        getLogger().w("INMEM pull -> queueUrl = [" + queueUrl + "]");

        validateQueues(queueUrl);

        List<DefaultMessage> queue = queues.get(queueUrl);

        if (queue.isEmpty()) {
            getLogger().w("There are not messages in the queue. leaving");
            // I could use NullMessage class also
            return null;
        }

        final List<DefaultMessage> queueWithPolled = queuesWithPolledMessages.get(queueUrl);

        final DefaultMessage internalMessage = queue.remove(0);

        final long timeout = setVisibilityTimeoutToMessage(visibilityTimeout, internalMessage);
        final String receiptHandle = setReceiptHandleForMessage(internalMessage);
        queueWithPolled.add(internalMessage);

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
                        timeout, TimeUnit.SECONDS);

        getLogger().w("PULLED internalMessage = " + internalMessage);
        return internalMessage;
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) {
        getLogger().w("INMEM delete -> queueName = [" + queueUrl + "], receiptHandle = [" + receiptHandle + "]");
        validateQueues(queueUrl);

        final List<DefaultMessage> queueWithPolled = queuesWithPolledMessages.get(queueUrl);

        queueWithPolled.removeIf(msg -> msg.getReceiptHandle().equals(receiptHandle));
    }

    private void validateQueues(String queueUrl) {
        if (queueUrl == null ||
                queueUrl.isEmpty() ||
                !queues.containsKey(queueUrl) ||
                !queuesWithPolledMessages.containsKey(queueUrl)) {
            throw new QueueDoesNotExistException("Queue " + queueUrl + " does not exist.");
        }
    }

    @Override
    public String createQueue(String queueName) {
        queues.computeIfAbsent(queueName, key -> new CopyOnWriteArrayList<>());
        queuesWithPolledMessages.computeIfAbsent(queueName, key -> new CopyOnWriteArrayList<>());

        return queueName;
    }

    @Override
    public void deleteQueue(String queueUrl) {
        queues.remove(queueUrl);
        queuesWithPolledMessages.remove(queueUrl);
    }
}