package com.example;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

public class InMemoryQueueService implements QueueService, Closeable {
    private static final Integer DEFAULT_VISIBILITY_TIMEOUT = 30;
    private static final String NOT_SET_RECEIPT_HANDLE = "";
    private static final int DEFAULT_RETRY_TIMEOUT_IN_MILLS = 100;

    private final Map<String, ConcurrentLinkedQueue<DefaultMessage>> queues =
            new ConcurrentHashMap<>();
    private final Map<String, ConcurrentLinkedQueue<DefaultMessage>> queuesWithPolledMessages =
            new ConcurrentHashMap<>();

    private final HashCalculator hashCalculator;
    private final ExecutorService executorService;

    public InMemoryQueueService(HashCalculator hashCalculator) {
        this.hashCalculator = hashCalculator;

        this.executorService = Executors.newSingleThreadExecutor();
    }

    public InMemoryQueueService(){
        this(new MD5HashCalculator());
    }

    public HashCalculator getHashCalculator() {
        return hashCalculator;
    }

    @Override
    public void push(String queueName, String messageBody)
            throws InterruptedException, IOException {

        System.out.println("push -> queueName = [" + queueName + "], messageBoy = [" + messageBody + "]");
        // message has the following struct
        // ATTEMPT_TO_DEQUEUE : VISIBLE_FROM_IN_MILLISECONDS : MESSAGE


        final DefaultMessage internalMessage =
                DefaultMessage.create(
                        generateMessageId(),
                        NOT_SET_RECEIPT_HANDLE,
                        messageBody,
                        getHashCalculator().calculate(messageBody));

        System.out.println("PUSHED internalMessage = " + internalMessage);

        queues.computeIfAbsent(queueName, q -> new ConcurrentLinkedQueue<>()).add(internalMessage);
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

        System.out.println("pull -> queueName = [" + queueName + "]");

        final ConcurrentLinkedQueue<DefaultMessage> queue =
                queues.getOrDefault(queueName, new ConcurrentLinkedQueue<>());
        final ConcurrentLinkedQueue<DefaultMessage> queueWithPolled =
                queuesWithPolledMessages.getOrDefault(queueName, new ConcurrentLinkedQueue<>());

        final DefaultMessage internalMessage = queue.poll();

        if (internalMessage == null) {
            return null;
        } else {
            final long timeout = this.calculateVisibility(visibilityTimeout);
            internalMessage.setVisibilityTimeout(timeout);

            System.out.println("timeout = " + timeout);

            internalMessage.setReceiptHandle();
            final String receiptHandle = internalMessage.getReceiptHandle();

            Future<?> task = executorService.submit(() -> {

                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("receiptHandle = " + receiptHandle);

                    DefaultMessage msg = queueWithPolled
                            .stream()
                            .filter(m -> m.getReceiptHandle().equals(receiptHandle))
                            .findFirst()
                            .orElseGet(() -> null);

                    // message was delete
                    if (msg == null) {
                        System.out.println("Message is null");
                        return;
                    } else {
                        System.out.println("let's wait and check again");
                        // let's wait and check again
                        try {
                            Thread.sleep(DEFAULT_RETRY_TIMEOUT_IN_MILLS);
                        } catch (InterruptedException e) {
                            // I was asked to stop
                            return;
                        }
                    }
                }

            });

            new Thread(() -> {
                try {
                    task.get(timeout, TimeUnit.MILLISECONDS);
                    System.out.println("Message was deleted");
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                    task.cancel(true);
                    // re-add message at the beginning
                    queueWithPolled.remove(internalMessage);
                    queue.add(internalMessage);
                    System.out.println("Message has been put back because it was not deleted.");
                }
            }).start();

            queueWithPolled.add(internalMessage);

            System.out.println("PULLED internalMessage = " + internalMessage);
            return internalMessage;
        }
    }

    @Override
    public void delete(String queueName, String receiptHandle) {
        System.out.println("delete -> queueName = [" + queueName + "], receiptHandle = [" + receiptHandle + "]");

        final ConcurrentLinkedQueue<DefaultMessage> queue =
                queues.getOrDefault(queueName, new ConcurrentLinkedQueue<>());
        final ConcurrentLinkedQueue<DefaultMessage> queueWithPolled =
                queuesWithPolledMessages.getOrDefault(queueName, new ConcurrentLinkedQueue<>());

        System.out.println(">1>>> queue.size() = " + queue.size());
        System.out.println(">1>>> queueWithPolled.size() = " + queueWithPolled.size());
        queueWithPolled.removeIf(msg -> msg.getReceiptHandle().equals(receiptHandle));
        System.out.println(">2>>> queue.size() = " + queue.size());
        System.out.println(">2>>> queueWithPolled.size() = " + queueWithPolled.size());
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }
}

