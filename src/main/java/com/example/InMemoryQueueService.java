package com.example;

import com.example.exceptions.QueueDoesNotExistException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class InMemoryQueueService extends BaseLocalQueueService {
    private static final Integer DEFAULT_VISIBILITY_TIMEOUT = 30;
    private static final String NOT_SET_RECEIPT_HANDLE = "";
    private static final int DEFAULT_RETRY_TIMEOUT_IN_MILLS = 100;

    private final Map<String, List<DefaultMessage>> queues =
            new ConcurrentHashMap<>();

    // Maybe not the optimal struct for queue, especially if the list is there -> O(N)
    private final Map<String, List<DefaultMessage>> queuesWithPolledMessages =
            new ConcurrentHashMap<>();

    // constructor for DI Container (f.e. Spring)
    public InMemoryQueueService(
            HashCalculator hashCalculator,
            ExecutorService executorService,
            Logger logger) {
        super(hashCalculator, executorService, logger);
    }

    public InMemoryQueueService(){
        this(new MD5HashCalculator(), Executors.newSingleThreadExecutor(), new SimpleConsoleLogger());
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

        getLogger().w("INMEM pull -> queueName = [" + queueUrl + "]");

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

        Future<?> task = getExecutorService().submit(() ->
                verifyVisibilityTimeoutOnDelete(receiptHandle, queueWithPolled));

        waitTillTaskExecutedAsync(queue, queueWithPolled, internalMessage, timeout, task);

        queueWithPolled.add(internalMessage);

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
            throw new QueueDoesNotExistException("");
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

    private void waitTillTaskExecutedAsync(
            final List<DefaultMessage> queue,
            final List<DefaultMessage> queueWithPolled,
            final DefaultMessage internalMessage,
            final long timeout,
            Future<?> task) {

        // simple way to do async wait..
        new Thread(() -> {
            try {
                task.get(timeout, TimeUnit.MILLISECONDS);
                getLogger().w("Message was deleted");
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
                task.cancel(true);
                // re-add message at the beginning
                queueWithPolled.remove(internalMessage);
                queue.add(0, internalMessage);
                getLogger().w("Message has been put back because it was not deleted.");
            }
        }).start();

    }

    private void verifyVisibilityTimeoutOnDelete(
            final String receiptHandle,
            final List<DefaultMessage> queueWithPolled) {

        while (!Thread.currentThread().isInterrupted()) {

            DefaultMessage msg = queueWithPolled
                    .stream()
                    .filter(m -> m.getReceiptHandle().equals(receiptHandle))
                    .findFirst()
                    .orElseGet(() -> null);

            // message was delete. Excellent!
            if (msg != null) {
                // let's wait and check again
                try {
                    Thread.sleep(DEFAULT_RETRY_TIMEOUT_IN_MILLS);
                } catch (InterruptedException e) {
                    // I was asked to stop
                    return;
                }
            } else {
                getLogger().w("Message is null");
                return;
            }
        }
    }
}