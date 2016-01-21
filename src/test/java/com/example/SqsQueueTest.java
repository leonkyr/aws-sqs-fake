package com.example;

import com.example.helpers.QueueServiceTester;
import org.junit.Test;

public class SqsQueueTest {

    private final QueueServiceTester tester = new QueueServiceTester();
    private final String FLAVOR = DefaultQueueServiceFactory.FLAVOR_PRODUCTION;

    @Test
    public void pushHappyPathBasicSqsTest() {

        tester.pushHappyPathBasicSqsTest(FLAVOR);
    }

    @Test
    public void pushHappyPath3In2OutSqsTest() {

        ///
        /// TODO: occasionally fails due to not guarantied FIFO property
        ///

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
    public void multipleProducersInDifferentThreadsHappyPathSqsTest() {

        tester.multipleProducersInDifferentThreadsHappyPathSqsTest(FLAVOR);
    }

    @Test
    public void multipleConsumersInDifferentThreadsHappyPath() {

        tester.multipleConsumersInDifferentThreadsHappyPath(FLAVOR);
    }
}

