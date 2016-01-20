package com.example;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class QueueTestWhen {
    private static final long DEFAULT_PUSH_TIMEOUT_IN_MILLS = 1000;
    private final QueueService queueService;
    private final String queueName;
    private Exception resultedException = null;
    private List<Message> pulledMessages;
    private List<String> pushedMessages;

    public QueueTestWhen(QueueService queueService, String queueName) {

        this.queueService = queueService;
        this.queueName = queueName;

        this.pulledMessages = new CopyOnWriteArrayList<>();
        this.pushedMessages = new CopyOnWriteArrayList<>();
    }

    private QueueService getQueueService() {
        return queueService;
    }

    private String getQueueName() {
        return queueName;
    }

    private List<Message> getPulledMessages() {
        return pulledMessages;
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

            getQueueService()
                    .push(getQueueName(), message);

            getPushedMessages().add(message);

        } catch (Exception e) {
            resultedException = e;
        }

        return this;
    }

    public QueueTestWhen pullAndSave() {
        try {
            Message message = getQueueService()
                    .pull(getQueueName());

            System.out.println("PULLED message = " + message);
            getPulledMessages().add(message);
        } catch (Exception e) {
            resultedException = e;
        }

        return this;
    }

    public QueueTestWhen delete(String messageBody) {

        Message messageToDelete = getPulledMessages()
                .stream()
                .filter(msg -> msg.getBody().equals(messageBody))
                .findFirst()
                .orElseGet(() -> null);

        try {
            if (messageToDelete != null) {
                System.out.println("messageToDelete.getReceiptHandle() = " + messageToDelete.getReceiptHandle());
                getQueueService()
                        .delete(
                                getQueueName(),
                                messageToDelete.getReceiptHandle());
            }

        } catch (Exception e) {
            resultedException = e;
        }


        return this;
    }

    public QueueTestThen then() {
        final QueueTestWhenResult queueTestWhenResult = new QueueTestWhenResult(
                getPulledMessages(),
                getPushedMessages(),
                getResultedException(),
                getQueueService(),
                getQueueName());
        return new QueueTestThen(queueTestWhenResult);
    }

    public QueueTestWhen waitToPassDelay(long waitTimeInMills) throws InterruptedException {
        Thread.sleep(waitTimeInMills);

        return this;
    }

    public QueueTestWhen pullAndSave(int visibilityTimeout) {
        try {
            Message message = getQueueService()
                    .pull(getQueueName(), visibilityTimeout);

            pulledMessages.add(message);
        } catch (Exception e) {
            resultedException = e;
        }

        return this;
    }

    public QueueTestWhen putMultipleMessageSimultaneously(
            Supplier<String> messageGenerator,
            final int simultaneouslyProducersCount,
            final int messageCountForAProducer) {

        ExecutorService executorService = Executors.newFixedThreadPool(simultaneouslyProducersCount);

        final Consumer<String> pushMessage = msg -> {
            try {
                getQueueService().push(getQueueName(), msg);
            } catch (Exception e) {
                resultedException = e;
            }
        };

        // I could use tasks, but could not make it work quickly
        for (int i = 0; i < simultaneouslyProducersCount; i++) {
            for (int j = 0; j < messageCountForAProducer; j++) {
                executorService.submit(() -> {
                    String message = messageGenerator.get();
                    pushMessage.accept(message);

                    getPushedMessages().add(message);
                    System.out.println("-----> message = " + message + ", getPushedMessages().size()" + getPushedMessages().size());
                });
            }
        }

        try {
            executorService.awaitTermination(DEFAULT_PUSH_TIMEOUT_IN_MILLS, TimeUnit.MILLISECONDS);
            System.out.println("Finished pushing messages");
        } catch (InterruptedException e) {
            resultedException = e;
        }

        return this;
    }
}

