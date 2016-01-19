package com.example;

import org.junit.Assert;

import java.util.List;

import static org.junit.Assert.fail;

public final class QueueTestThen {
    private final QueueTestWhenResult whenResult;

    public QueueTestThen(QueueTestWhenResult whenResult) {

        this.whenResult = whenResult;
    }

    private List<String> getMessages() {
        return whenResult.getMessages();
    }

    private Exception getResultedException() {
        return whenResult.getResultedException();
    }

    private QueueService getQueueService() {
        return whenResult.getQueueService();
    }

    public QueueTestThen assertSaveMessage(String message) {

        if (getMessages().size() == 0) {
            fail("There are no message pulled");
        }

        Assert.assertEquals(getMessages().get(0), message);

        return this;
    }

    public QueueTestThen and() {
        return this;
    }

    public QueueTestThen assertQueueHasNotMessages() {

        try {
            final String message = getQueueService().pull(whenResult.getQueueName());
            Assert.assertNull(message);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        return this;
    }
}
