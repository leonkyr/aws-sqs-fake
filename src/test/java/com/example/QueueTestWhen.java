package com.example;

import java.util.ArrayList;
import java.util.List;

public final class QueueTestWhen {
    private final QueueService queueService;
    private final String queueName;
    private Exception resultedException;
    private List<String> messages;

    public QueueTestWhen(QueueService queueService, String queueName) {

        this.queueService = queueService;
        this.queueName = queueName;

        this.messages = new ArrayList<>();
    }

    public QueueTestWhen put(String message) {

        try {

            getQueueService()
                    .push(getQueueName(), 0, message);

        } catch (Exception e) {
            resultedException = e;
        }

        return this;
    }

    private QueueService getQueueService() {
        return queueService;
    }

    private String getQueueName() {
        return queueName;
    }

    private List<String> getMessages() {
        return messages;
    }

    private Exception getResultedException() {
        return resultedException;
    }

    public QueueTestWhen and() {
        return this;
    }

    public QueueTestWhen pullAndSave() {
        try {
            messages.add(
                    getQueueService()
                            .pull(getQueueName()));
        } catch (Exception e) {
            resultedException = e;
        }

        return this;
    }

    public QueueTestWhen delete(String message) {

        try {
            getQueueService()
                    .delete(message);
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
}

