package com.example;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InMemoryQueueService implements QueueService {

    private static final String DEFAULT_RECEIPT_HANDLE = "";
    private final Map<String, ConcurrentLinkedQueue<Message>> queues =
            new ConcurrentHashMap<>();
    private final Map<String, ConcurrentLinkedQueue<Message>> pulledMessages =
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
    public void push(String queueName, String message) throws InterruptedException, IOException {
        System.out.println("push -> queueName = [" + queueName + "], message = [" + message + "]");
        // message has the following struct
        // ATTEMPT_TO_DEQUEUE : VISIBLE_FROM_IN_MILLISECONDS : MESSAGE

        // implementation of delay of visibility when the message is pushed
//        final long visibility = calculateVisibility(delayInSeconds);

        final Message internalMessage =
                Message.create(
                        generateMessageId(),
                        DEFAULT_RECEIPT_HANDLE,
                        message,
                        getHashCalculator().calculate(message));

        queues.computeIfAbsent(queueName, q -> new ConcurrentLinkedQueue<>()).add(internalMessage);
    }

    @Override
    public Message pull(String queueName) throws InterruptedException, IOException {
        System.out.println("pull -> queueName = [" + queueName + "]");

        final Message internalMessage = queues.getOrDefault(queueName, new ConcurrentLinkedQueue<>()).poll();

        if (internalMessage == null) {
            return null;
        } else {
            pulledMessages.computeIfAbsent(queueName, q -> new ConcurrentLinkedQueue<>()).add(internalMessage);
            return internalMessage;
        }
    }

    @Override
    public void delete(String queueName, String message) {
        System.out.println("delete -> queueName = [" + queueName + "], message = [" + message + "]");

        String hash = getHashCalculator().calculate(message);

        final ConcurrentLinkedQueue<Message> queue = pulledMessages.getOrDefault(queueName, new ConcurrentLinkedQueue<>());

        Message internalMessage = queue.stream().filter(msg -> msg.getMD5OfBody().equals(hash)).findFirst().orElseGet(() -> null);

        if (!queue.remove(internalMessage)) {

            queues.computeIfAbsent(queueName, q -> new ConcurrentLinkedQueue<>()).add(internalMessage);
        }
    }
}

