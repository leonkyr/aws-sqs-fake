package com.example;

import org.junit.Before;
import org.junit.Test;

public class InMemoryQueueTest {

    private static final String LOCAL = "local";
    private static final String QUEUE_NAME = "canva-rocks";
    private static final int MESSAGE_VISIBILITY_TIMEOUT_IN_SECONDS = 1;
    private static final int DEFAULT_WAIT_TIME_IN_MILLS = 10*1000;
    private MessageGenerator messageGenerator = new MessageGenerator();

    @Before
    public void before() {
    }

    @Test
    public void pushHappyPathBasicTest() {

        QueueTestGiven given = new QueueTestGiven();

        String message = messageGenerator.generate();

        given
                .setEnvironmentFlavor(LOCAL)
                .and()
                .setQueueName(QUEUE_NAME).

        when()
                .put(message)
                .and()
                .pullAndSave()
                .and()
                .delete(message).

        then()
                .assertThereIsNoException()
                .and()
                .assertSavedMessage(message)
                .and()
                .assertQueueHasNotMessages();
    }


    @Test
    public void pushHappyPath3In2OutTest() {

        QueueTestGiven given = new QueueTestGiven();

        String message1 = messageGenerator.generate();
        String message2 = messageGenerator.generate();
        String message3 = messageGenerator.generate();

        given
                .setEnvironmentFlavor(LOCAL)
                .and()
                .setQueueName(QUEUE_NAME).

        when()
                .put(message1)
                .and()
                .put(message2)
                .and()
                .put(message3)
                .and()
                .pullAndSave()
                .and()
                .pullAndSave()
                .and()
                .delete(message1)
                .and()
                .delete(message2).

        then()
                .assertThereIsNoException()
                .and()
                .assertSavedMessage(message1)
                .and()
                .assertSavedMessage(message2)
                .and()
                .assertQueueHasMessages();
    }

    @Test
    public void notDeletedMessagePutBackSuccessfully() throws InterruptedException {

        QueueTestGiven given = new QueueTestGiven();

        String message = messageGenerator.generate();

        given
                .setEnvironmentFlavor(LOCAL)
                .and()
                .setQueueName(QUEUE_NAME).

        when()
                .put(message)
                .and()
                .pullAndSave(MESSAGE_VISIBILITY_TIMEOUT_IN_SECONDS)
                .and()
                .waitToPassDelay(DEFAULT_WAIT_TIME_IN_MILLS).

        then()
                .assertThereIsNoException()
                .and()
                .assertQueueHasMessages();
    }
}
