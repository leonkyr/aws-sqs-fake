package com.example;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

public class InMemoryQueueService implements QueueService {
    private static final Integer DEFAULT_VISIBILITY_TIMEOUT = 30;
    private static final String NOT_SET_RECEIPT_HANDLE = "";
    private static final int DEFAULT_RETRY_TIMEOUT = 20;

    private final Map<String, ConcurrentLinkedQueue<DefaultMessage>> queues =
            new ConcurrentHashMap<>();
    private final Map<String, ConcurrentLinkedQueue<DefaultMessage>> pulledMessages =
            new ConcurrentHashMap<>();
    private final HashCalculator hashCalculator;

    public InMemoryQueueService(HashCalculator hashCalculator) {
        this.hashCalculator = hashCalculator;
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

        System.out.println("pull -> queueName = [" + queueName + "]");

        final DefaultMessage internalMessage = queues.getOrDefault(queueName, new ConcurrentLinkedQueue<>()).poll();

        if (internalMessage == null) {
            return null;
        } else {
            internalMessage.setReceiptHandle();
            final long timeout = this.calculateVisibility(visibilityTimeout);
            internalMessage.setVisibilityTimeout(timeout);

            System.out.println("timeout = " + timeout);

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<?> task = executorService.submit(() -> {
                final ConcurrentLinkedQueue<DefaultMessage> queue =
                        pulledMessages.getOrDefault(queueName, new ConcurrentLinkedQueue<>());

                final String receiptHandle = internalMessage.getReceiptHandle();

                while (true) {
                    System.out.println("receiptHandle = " + receiptHandle);
                    DefaultMessage msg = queue
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
                            Thread.sleep(DEFAULT_RETRY_TIMEOUT);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

            });

            try {
                task.get(timeout, TimeUnit.MILLISECONDS);
                System.out.println("Message was deleted");
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
                queues.getOrDefault(queueName, new ConcurrentLinkedQueue<>()).add(internalMessage);
                pulledMessages.computeIfAbsent(queueName, q -> new ConcurrentLinkedQueue<>()).remove(internalMessage);
                System.out.println("Message has been put back because it was not deleted.");
            }

            pulledMessages.computeIfAbsent(queueName, q -> new ConcurrentLinkedQueue<>()).add(internalMessage);
            return internalMessage;
        }
    }

    @Override
    public void delete(String queueName, String receiptHandle) {
        System.out.println("delete -> queueName = [" + queueName + "], receiptHandle = [" + receiptHandle + "]");

        final ConcurrentLinkedQueue<DefaultMessage> queue = pulledMessages.getOrDefault(queueName, new ConcurrentLinkedQueue<>());

        DefaultMessage internalMessage = queue
                .stream()
                .filter(msg -> msg.getReceiptHandle().equals(receiptHandle))
                .findFirst()
                .orElseGet(() -> null);

        if (internalMessage != null && !queue.remove(internalMessage)) {
            queues.computeIfAbsent(queueName, q -> new ConcurrentLinkedQueue<>()).add(internalMessage);
        }
    }
}

