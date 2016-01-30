package com.example.helpers;

import com.example.Message;
import com.example.QueueService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static org.junit.Assert.fail;

public final class QueueTestWhen {
    private static final long DEFAULT_PUSH_TIMEOUT_IN_MILLS = 1000;
    private static final long DEFAULT_PULL_TIMEOUT_IN_MILLS = 5 * 1000;
    private final QueueService queueService;
    private final String queueUrl;
    private Exception resultedException = null;
    private List<Message> pulledMessages;
    private List<String> pushedMessages;
    private List<Message> deletedMessages;

    public QueueTestWhen(QueueService queueService, String queueUrl) {

        this.queueService = queueService;
        this.queueUrl = queueUrl;

        this.pulledMessages = new CopyOnWriteArrayList<>();
        this.pushedMessages = new CopyOnWriteArrayList<>();
        this.deletedMessages = new CopyOnWriteArrayList<>();
    }

    private QueueService getQueueService() {
        return queueService;
    }

    private String getQueueUrl() {
        return queueUrl;
    }

    private List<Message> getPulledMessages() {
        return pulledMessages;
    }

    private List<Message> getDeletedMessages() {
        return deletedMessages;
    }

    public List<String> getPushedMessages() {
        return pushedMessages;
    }

    private Exception getResultedException() {
        return resultedException;
    }

    public QueueTestWhen and() {
        return this;
    }

    public QueueTestWhen put(String message) {

        try {

            getQueueService().push(getQueueUrl(), message);

            getPushedMessages().add(message);

        } catch (Exception e) {
            resultedException = e;
            e.printStackTrace();
        }

        return this;
    }

    public QueueTestWhen pullAndSave() {

        try {
            Message message = getQueueService().pull(getQueueUrl());

            System.out.println(">>> Stored PULLED message = " + message);
            getPulledMessages().add(message);
        } catch (Exception e) {
            resultedException = e;
            e.printStackTrace();
        }

        return this;
    }

    public QueueTestWhen delete(String messageBody) {

        Message messageToDelete = getPulledMessages()
                .stream()
//                .peek(System.out::println)
                .filter(msg -> msg.getBody().equals(messageBody))
                .findFirst()
                .orElseGet(() -> null);

        return delete(messageToDelete);
    }

    public QueueTestWhen delete() {

        Message messageToDelete = null;

        if (getPulledMessages().size() != 0) {
            messageToDelete = getPulledMessages().get(0);
        }

        return delete(messageToDelete);
    }

    public QueueTestWhen deleteWithForce() {
        try {
            getQueueService().delete(
                    getQueueUrl(),
                    UUID.randomUUID().toString());
        } catch (Exception e) {
            resultedException = e;
            e.printStackTrace();
        }

        return this;
    }

    private QueueTestWhen delete(Message messageToDelete) {
        System.out.println("TEST DELETE -> messageToDelete = [" + messageToDelete + "]");

        try {
            if (messageToDelete != null) {
                getQueueService().delete(
                        getQueueUrl(),
                        messageToDelete.getReceiptHandle());

                getPulledMessages().remove(messageToDelete);
                getDeletedMessages().add(messageToDelete);
            }
            else {
                fail("Could not find message to delete.");
            }

        } catch (Exception e) {
            resultedException = e;
            e.printStackTrace();
        }

        return this;
    }

    public QueueTestThen then() {
        final QueueTestWhenResult queueTestWhenResult = new QueueTestWhenResult(
                getDeletedMessages(),
                getPushedMessages(),
                getResultedException(),
                getQueueService(),
                getQueueUrl());
        return new QueueTestThen(queueTestWhenResult);
    }

    public QueueTestWhen waitToPassDelay(long waitTimeInMills) throws InterruptedException {
        Thread.sleep(waitTimeInMills);

        return this;
    }

    public QueueTestWhen pullAndSave(int visibilityTimeout) {
        try {
            Message message = getQueueService()
                    .pull(getQueueUrl(), visibilityTimeout);

            pulledMessages.add(message);
        } catch (Exception e) {
            resultedException = e;
            e.printStackTrace();
        }

        return this;
    }

    public QueueTestWhen putMultipleMessagesSimultaneously(
            Supplier<String> messageGenerator,
            final int simultaneouslyProducersCount,
            final int messageCountForAProducer) {

        ExecutorService executorService = Executors.newFixedThreadPool(simultaneouslyProducersCount);

        List<String> messages = new ArrayList<>();
        for (int i=0; i<simultaneouslyProducersCount*messageCountForAProducer; i++) {
            messages.add(messageGenerator.get());
        }

        messages.forEach(this::publishMessage);

        executorService.shutdown();
        try {
            final boolean done = executorService.awaitTermination(DEFAULT_PUSH_TIMEOUT_IN_MILLS, TimeUnit.MILLISECONDS);
            System.out.println("Finished pushing messages. DONE?" + done);
        } catch (InterruptedException e) {
            resultedException = e;
        }

        return this;
    }

    private void publishMessage(String message) {
        System.out.println("-----> message = " + message + ", getPushedMessages().size()" + getPushedMessages().size());
        try {
            getQueueService().push(getQueueUrl(), message);
            getPushedMessages().add(message);
        } catch (Exception e) {
            resultedException = e;
            e.printStackTrace();
        }
    }

    public QueueTestWhen putMultipleMessages(Supplier<String> messageGenerator, final int messageCount) {

        for (int i = 0; i < messageCount; i++) {
            put(messageGenerator.get());
        }
        return this;
    }

    public QueueTestWhen consumeMultipleMessageSimultaneously(
            final int consumersCount,
            final int messageCountPerConsumer) {

        ExecutorService executorService = Executors.newFixedThreadPool(consumersCount);


        for (int i=0; i<consumersCount; i++) {
            for (int j = 0; j < messageCountPerConsumer; j++) {
                executorService.submit((Callable<QueueTestWhen>) this::pullAndSave);
            }
        }

        executorService.shutdown();
        try {
            final boolean done = executorService.awaitTermination(DEFAULT_PULL_TIMEOUT_IN_MILLS, TimeUnit.MILLISECONDS);
            System.out.println("Finished pulling messages. DONE?" + done);
        } catch (InterruptedException e) {
            resultedException = e;
        }

        return this;
    }
}

