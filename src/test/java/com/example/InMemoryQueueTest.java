package com.example;

import org.junit.Before;
import org.junit.Test;

public class InMemoryQueueTest {

    private static final String LOCAL = "local";
    private static final String HAPPY_PATH_BASIC_TEST_QUEUE = "q-01";
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
                .setQueueName(HAPPY_PATH_BASIC_TEST_QUEUE).

        when()
                .put(message)
                .and()
                .pullAndSave()
                .and()
                .delete(message).

        then()
                .assertSaveMessage(message)
                .and()
                .assertQueueHasNotMessages();
    }


}
