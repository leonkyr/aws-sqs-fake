package com.example;

import com.example.helpers.QueueServiceTester;
import org.junit.Test;

public class FileQueueTest {

    private final QueueServiceTester tester = new QueueServiceTester();
    private final String FLAVOR = DefaultQueueServiceFactory.FLAVOR_INTEGRATION;

    @Test
    public void pushHappyPathBasicSqsTest() {

        tester.pushHappyPathBasicSqsTest(FLAVOR);
    }

    @Test
    public void pushHappyPath3In2OutSqsTest() {

        tester.pushHappyPath3In2OutSqsTest(FLAVOR);
    }

    @Test
    public void pushHappyPath2In2OutSqsTest() {

        tester.pushHappyPath2In2OutSqsTest(FLAVOR);
    }

    @Test
    public void notDeletedMessagePutBackSuccessfullySqsTest() throws InterruptedException {

        tester.notDeletedMessagePutBackSuccessfullySqsTest(FLAVOR);
    }

    @Test
    public void multiplyProducersInDifferentThreadsHappyPathSqsTest() {

        tester.multipleProducersInDifferentThreadsHappyPathSqsTest(FLAVOR);
    }

    @Test
    public void multipleConsumersInDifferentThreadHappyPath() {

        tester.multipleConsumersInDifferentThreadsHappyPath(FLAVOR);
    }
}

