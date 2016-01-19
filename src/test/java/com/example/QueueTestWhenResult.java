package com.example;

import java.util.List;

public class QueueTestWhenResult {

    private final List<Message> messages;
    private final Exception resultedException;
    private final QueueService queueService;
    private final String queueName;

    public QueueTestWhenResult(
            List<Message> messages,
            Exception resultedException,
            QueueService queueService,
            String queueName) {

        this.messages = messages;
        this.resultedException = resultedException;
        this.queueService = queueService;
        this.queueName = queueName;
    }

    public List<Message> getMessages() {
        return messages;
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
}
