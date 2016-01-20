package com.example;

import java.util.List;

public class QueueTestWhenResult {

    private final List<Message> pulledMessages;
    private final List<String> pushedMessages;
    private final Exception resultedException;
    private final QueueService queueService;
    private final String queueName;

    public QueueTestWhenResult(
            List<Message> pulledMessages,
            List<String> pushedMessages,
            Exception resultedException,
            QueueService queueService,
            String queueName) {

        this.pulledMessages = pulledMessages;
        this.pushedMessages = pushedMessages;
        this.resultedException = resultedException;
        this.queueService = queueService;
        this.queueName = queueName;
    }

    public List<Message> getPulledMessages() {
        return pulledMessages;
    }

    public Exception getResultedException() {
        return resultedException;
    }

    public QueueService getQueueService() {
        return queueService;
    }

    public String getQueueName() {
        return queueName;
    }

    public List<String> getPushedMessages() {
        return pushedMessages;
    }
}
