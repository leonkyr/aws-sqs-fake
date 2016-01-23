package com.example.helpers;

import com.example.Message;
import com.example.QueueService;
import org.junit.Assert;

import java.util.List;

import static org.junit.Assert.fail;

public final class QueueTestThen {
    private final QueueTestWhenResult whenResult;

    public QueueTestThen(QueueTestWhenResult whenResult) {

        this.whenResult = whenResult;
    }

    private List<Message> getDeletedMessages() {
        return whenResult.getDeletedMessages();
    }

    private List<String> getPushedMessages() {
        return whenResult.getPushedMessages();
    }

    private Exception getResultedException() {
        return whenResult.getResultedException();
    }

    private QueueService getQueueService() {
        return whenResult.getQueueService();
    }

    public QueueTestThen assertSavedAndDeletedMessage(String messageBody) {

        System.out.println("TEST ASSERT -> message = [" + messageBody + "]");

        if (getDeletedMessages().size() == 0) {
            fail("There are no deleted messages");
        }

        Assert.assertSame("The message count is not the same.", 1,
                (int) getDeletedMessages()
                        .stream()
                        .filter(msg -> msg.getBody().equals(messageBody))
                        .count());

        return this;
    }

    public QueueTestThen and() {
        return this;
    }

    public QueueTestThen assertQueueHasNotMessages() {

        try {
            final Message message = getQueueService().pull(whenResult.getQueueName());
            Assert.assertNull("The message with body ["+(message != null ? message.getBody() : "<NULL>")+"] was returned", message);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        return this;
    }

    public QueueTestThen assertThereIsNoException() {
        if (getResultedException() != null) {
            getResultedException().printStackTrace();
        }
        Assert.assertNull(getResultedException());

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

    public QueueTestThen assertQueueHasMessageSize(int expectedQueueSize) {

        Assert.assertSame("The message count is not the same.", expectedQueueSize,
                getPushedMessages().size());

        return this;
    }


}
