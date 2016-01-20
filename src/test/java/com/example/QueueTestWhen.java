package com.example;

import java.util.ArrayList;
import java.util.List;

public final class QueueTestWhen {
    private final QueueService queueService;
    private final String queueName;
    private Exception resultedException = null;
    private List<Message> messages;

    public QueueTestWhen(QueueService queueService, String queueName) {

        this.queueService = queueService;
        this.queueName = queueName;

        this.messages = new ArrayList<>();
    }

    private QueueService getQueueService() {
        return queueService;
    }

    private String getQueueName() {
        return queueName;
    }

    private List<Message> getMessages() {
        return messages;
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

        } catch (Exception e) {
            resultedException = e;
        }

        return this;
    }

    public QueueTestWhen pullAndSave() {
        try {
            Message message = getQueueService()
                    .pull(getQueueName());

            messages.add(message);
        } catch (Exception e) {
            resultedException = e;
        }

        return this;
    }

    public QueueTestWhen delete(String messageBody) {

        Message messageToDelete = messages
                .stream()
                .filter(msg -> msg.getBody().equals(messageBody))
                .findFirst()
                .orElseGet(() -> null);

        try {
            if (messageToDelete != null) {
                getQueueService()
                        .delete(getQueueName(), messageToDelete.getReceiptHandle());
            }

        } catch (Exception e) {
            resultedException = e;
        }


        return this;
    }

    public QueueTestThen then() {
        final QueueTestWhenResult queueTestWhenResult = new QueueTestWhenResult(
                getMessages(),
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

            messages.add(message);
        } catch (Exception e) {
            resultedException = e;
        }

        return this;
    }
}

