package com.example;

import org.junit.Assert;

import java.util.List;

import static org.junit.Assert.fail;

public final class QueueTestThen {
    private final QueueTestWhenResult whenResult;

    public QueueTestThen(QueueTestWhenResult whenResult) {

        this.whenResult = whenResult;
    }

    private List<Message> getMessages() {
        return whenResult.getMessages();
    }

    private Exception getResultedException() {
        return whenResult.getResultedException();
    }

    private QueueService getQueueService() {
        return whenResult.getQueueService();
    }

    public QueueTestThen assertSavedMessage(String message) {

        if (getMessages().size() == 0) {
            fail("There are no message pulled");
        }

        Assert.assertSame("The message was not found.", 1,
                (int) getMessages()
                        .stream()
                        .filter(msg -> msg.getBody().equals(message))
                        .count());

        return this;
    }

    public QueueTestThen and() {
        return this;
    }

    public QueueTestThen assertQueueHasNotMessages() {

        try {
            final Message message = getQueueService().pull(whenResult.getQueueName());
            Assert.assertNull(message);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        return this;
    }

    public QueueTestThen assertThereIsNoException() {
        if (whenResult.getResultedException() != null) {
            whenResult.getResultedException().printStackTrace();
        }
        Assert.assertNull(whenResult.getResultedException());

        return this;
    }

    public QueueTestThen assertQueueHasMessages() {

        try {
            final Message message = getQueueService().pull(whenResult.getQueueName());
            Assert.assertNotNull("Queue does not have messages", message);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        return this;
    }
}
