package com.example;

import java.util.ArrayList;
import java.util.List;

public final class QueueTestWhen {
    private final QueueService queueService;
    private final String queueName;
    private Exception resultedException = null;
    private List<Message> pulledMessages;
    private List<String> pushedMessages;

    public QueueTestWhen(QueueService queueService, String queueName) {

        this.queueService = queueService;
        this.queueName = queueName;

        this.pulledMessages = new ArrayList<>();
        this.pushedMessages = new ArrayList<>();
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
}

