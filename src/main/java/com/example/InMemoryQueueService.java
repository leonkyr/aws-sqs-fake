package com.example;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

public class InMemoryQueueService extends BaseLocalQueueService implements Closeable {
    private static final Integer DEFAULT_VISIBILITY_TIMEOUT = 30;
    private static final String NOT_SET_RECEIPT_HANDLE = "";
    private static final int DEFAULT_RETRY_TIMEOUT_IN_MILLS = 100;

    private final Map<String, ConcurrentLinkedQueue<DefaultMessage>> queues =
            new ConcurrentHashMap<>();

    // Maybe not the optimal struct for queue, especially if the list is there -> O(N)
    private final Map<String, ConcurrentLinkedQueue<DefaultMessage>> queuesWithPolledMessages =
            new ConcurrentHashMap<>();

    private final HashCalculator hashCalculator;
    private final Logger logger;
    private final ExecutorService executorService;

    // constructor for DI Container (f.e. Spring)
    public InMemoryQueueService(HashCalculator hashCalculator, Logger logger) {

        this.hashCalculator = hashCalculator;
        this.logger = logger;

        this.executorService = Executors.newSingleThreadExecutor();
    }

    public InMemoryQueueService(){
        this(new MD5HashCalculator(), new SimpleConsoleLogger());
    }

    public HashCalculator getHashCalculator() {
        return hashCalculator;
    }

    @Override
    public void push(String queueName, String messageBody)
            throws InterruptedException, IOException {

        logger.w("INMEM push -> queueName = [" + queueName + "], messageBoy = [" + messageBody + "]");

        final DefaultMessage internalMessage =
                DefaultMessage.create(
                        generateMessageId(),
                        NOT_SET_RECEIPT_HANDLE,
                        messageBody,
                        getHashCalculator().calculate(messageBody));

        logger.w("PUSHED internalMessage = " + internalMessage);

        ConcurrentLinkedQueue<DefaultMessage> queue = getOrCreateQueueByName(queueName);

        queue.add(internalMessage);

        logger.w("Added to the queue the message. Queue size = " + queue.size());
    }

    @Override
    public Message pull(String queueName)
            throws InterruptedException, IOException {

        return pull(queueName, DEFAULT_VISIBILITY_TIMEOUT);
    }

    @Override
    public Message pull(String queueName, Integer visibilityTimeout)
            throws InterruptedException, IOException {
        // we don't care that a consumer make consume multiple message at the same time

        logger.w("INMEM pull -> queueName = [" + queueName + "]");

        final ConcurrentLinkedQueue<DefaultMessage> queue =
                getQueueByName(queueName);

        if (queue.isEmpty()) {
            logger.w("There are not messages in the queue. leaving");
            // I could use NullMessage class also..
            return null;
        }

        final ConcurrentLinkedQueue<DefaultMessage> queueWithPolled =
                getQueuesWithPolledMessagesByName(queueName);

        final DefaultMessage internalMessage = queue.poll();

        final long timeout = setVisibilityTimeoutToMessage(visibilityTimeout, internalMessage);
        final String receiptHandle = setReceiptHandleForMessage(internalMessage);

        Future<?> task = executorService.submit(() ->
                verifyVisibilityTimeoutOnDelete(receiptHandle, queueWithPolled));

        waitTillTaskExecutedAsync(queue, queueWithPolled, internalMessage, timeout, task);

        queueWithPolled.add(internalMessage);

        logger.w("PULLED internalMessage = " + internalMessage);
        return internalMessage;
    }

    @Override
    public void delete(String queueName, String receiptHandle) {
        logger.w("INMEM delete -> queueName = [" + queueName + "], receiptHandle = [" + receiptHandle + "]");

        final ConcurrentLinkedQueue<DefaultMessage> queueWithPolled =
                getQueuesWithPolledMessagesByName(queueName);

        logger.w("Polled queue size BEFORE delete is " + queueWithPolled.size());
        queueWithPolled.removeIf(msg -> msg.getReceiptHandle().equals(receiptHandle));
        logger.w("Polled queue size AFTER delete is " + queueWithPolled.size());
    }

    private ConcurrentLinkedQueue<DefaultMessage> getOrCreateQueueByName(String queueName) {
        return queues.computeIfAbsent(queueName, q -> new ConcurrentLinkedQueue<>());
    }

    private ConcurrentLinkedQueue<DefaultMessage> getQueueByName(String queueName) {
        return queues.getOrDefault(queueName, new ConcurrentLinkedQueue<>());
    }

    private ConcurrentLinkedQueue<DefaultMessage> getQueuesWithPolledMessagesByName(String queueName) {
        return queuesWithPolledMessages.getOrDefault(queueName, new ConcurrentLinkedQueue<>());
    }

    private void waitTillTaskExecutedAsync(
            final ConcurrentLinkedQueue<DefaultMessage> queue,
            final ConcurrentLinkedQueue<DefaultMessage> queueWithPolled,
            final DefaultMessage internalMessage,
            final long timeout,
            Future<?> task) {

        // simple way to do async wait..
        new Thread(() -> {
            try {
                task.get(timeout, TimeUnit.MILLISECONDS);
                logger.w("Message was deleted");
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
                task.cancel(true);
                // re-add message at the beginning
                queueWithPolled.remove(internalMessage);
                queue.add(internalMessage);
                logger.w("Message has been put back because it was not deleted.");
            }
        }).start();

    }

    private void verifyVisibilityTimeoutOnDelete(
            final String receiptHandle,
            final ConcurrentLinkedQueue<DefaultMessage> queueWithPolled) {

        while (!Thread.currentThread().isInterrupted()) {

            DefaultMessage msg = queueWithPolled
                    .stream()
                    .filter(m -> m.getReceiptHandle().equals(receiptHandle))
                    .findFirst()
                    .orElseGet(() -> null);

            // message was delete
            if (msg != null) {
                // let's wait and check again
                try {
                    Thread.sleep(DEFAULT_RETRY_TIMEOUT_IN_MILLS);
                } catch (InterruptedException e) {
                    // I was asked to stop
                    return;
                }
            } else {
                logger.w("Message is null");
                return;
            }
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }
}