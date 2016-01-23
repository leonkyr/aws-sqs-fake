package com.example.helpers;

public class QueueServiceTester {

    private static final int MESSAGE_VISIBILITY_TIMEOUT_IN_SECONDS = 1;
    private static final int DEFAULT_WAIT_TIME_IN_MILLS = 7*1000;
    private static final int PRODUCERS_COUNT = 10;
    private static final int MESSAGE_COUNT_FOR_A_PRODUCER = 10;
    private static final int CONSUMERS_COUNT = 10;
    private static final int MESSAGE_COUNT_FOR_A_CONSUMER = 10;
    private final MessageGenerator messageGenerator = new MessageGenerator();
    private final QueueNameGenerator queueNameGenerator = new QueueNameGenerator();

    public void pushHappyPathBasicTest(String flavor) {

        QueueTestGiven given = new QueueTestGiven();

        String message = messageGenerator.generate();

        final String queueName = queueNameGenerator.generate();

        given
                .setEnvironmentFlavor(flavor)
                .and()
                .setQueueName(queueName).

        when()
                .put(message)
                .and()
                .pullAndSave()
                .and()
                .delete(message).

        then()
                .assertThereIsNoException()
                .and()
                .assertSavedAndDeletedMessage(message)
                .and()
                .assertQueueHasNotMessages();
    }

    public void pushHappyPath3In2OutTest(String flavor) {

        ///
        /// TODO: occasionally fails due to not guarantied FIFO property (SQS case)
        ///

        QueueTestGiven given = new QueueTestGiven();

        String message1 = messageGenerator.generate();
        String message2 = messageGenerator.generate();
        String message3 = messageGenerator.generate();

        final String queueName = queueNameGenerator.generate();

        given
                .setEnvironmentFlavor(flavor)
                .and()
                .setQueueName(queueName).

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
                .delete()
                .and()
                .delete().

        then()
                .assertThereIsNoException()
                .and()
                .assertSavedAndDeletedMessage(message1)
                .and()
                .assertSavedAndDeletedMessage(message2)
                .and()
                .assertQueueHasMessages();
    }

    public void pushHappyPath2In2OutTest(String flavor) {

        QueueTestGiven given = new QueueTestGiven();

        String message1 = messageGenerator.generate();
        String message2 = messageGenerator.generate();

        final String queueName = queueNameGenerator.generate();

        given
                .setEnvironmentFlavor(flavor)
                .and()
                .setQueueName(queueName).

        when()
                .put(message1)
                .and()
                .put(message2)
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
                .assertSavedAndDeletedMessage(message1)
                .and()
                .assertSavedAndDeletedMessage(message2)
                .and()
                .assertQueueHasNotMessages();
    }

    public void notDeletedMessagePutBackSuccessfullyTest(String flavor) throws InterruptedException {

        QueueTestGiven given = new QueueTestGiven();

        String message = messageGenerator.generate();
        final String queueName = queueNameGenerator.generate();

        given
                .setEnvironmentFlavor(flavor)
                .and()
                .setQueueName(queueName).

        when()
                .put(message)
                .and()
                .pullAndSave(MESSAGE_VISIBILITY_TIMEOUT_IN_SECONDS)
                .and()
                .waitToPassDelay(DEFAULT_WAIT_TIME_IN_MILLS)
                .and()
                .pullAndSave()
                .and()
                .delete(message).

        then()
                .assertThereIsNoException()
                .and()
                .assertQueueHasNotMessages();
    }

    public void multipleProducersInDifferentThreadsHappyPathTest(String flavor) {

        QueueTestGiven given = new QueueTestGiven();

        final String queueName = queueNameGenerator.generate();

        given
                .setEnvironmentFlavor(flavor)
                .and()
                .setQueueName(queueName).

        when()
                .putMultipleMessagesSimultaneously(
                        messageGenerator::generate,
                        PRODUCERS_COUNT,
                        MESSAGE_COUNT_FOR_A_PRODUCER).

        then()
                .assertThereIsNoException()
                .and()
                .assertQueueHasMessageSize(PRODUCERS_COUNT*MESSAGE_COUNT_FOR_A_PRODUCER);
    }

    public void multipleConsumersInDifferentThreadsHappyPathTest(String flavor) {
        QueueTestGiven given = new QueueTestGiven();

        final int messageCount = PRODUCERS_COUNT*MESSAGE_COUNT_FOR_A_PRODUCER;
        final String queueName = queueNameGenerator.generate();

        given
                .setEnvironmentFlavor(flavor)
                .and()
                .setQueueName(queueName).

        when()
                .putMultipleMessages(
                        messageGenerator::generate,
                        messageCount)
                .and()
                .consumeMultipleMessageSimultaneously(
                        CONSUMERS_COUNT,
                        MESSAGE_COUNT_FOR_A_CONSUMER).

        then()
                .assertThereIsNoException()
                .and()
                .assertQueueHasNotMessages();
    }
}
