package com.example.helpers;

import com.example.Message;
import com.example.QueueService;

import java.util.List;

public class QueueTestWhenResult {

    private final List<Message> deletedMessages;
    private final List<String> pushedMessages;
    private final Exception resultedException;
    private final QueueService queueService;
    private final String queueUrl;

    public QueueTestWhenResult(
            List<Message> deletedMessages,
            List<String> pushedMessages,
            Exception resultedException,
            QueueService queueService,
            String queueUrl) {

        this.deletedMessages = deletedMessages;
        this.pushedMessages = pushedMessages;
        this.resultedException = resultedException;
        this.queueService = queueService;
        this.queueUrl = queueUrl;
    }

    public List<Message> getDeletedMessages() {
        return deletedMessages;
    }

    public Exception getResultedException() {
        return resultedException;
    }

    public QueueService getQueueService() {
        return queueService;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public List<String> getPushedMessages() {
        return pushedMessages;
    }
}
