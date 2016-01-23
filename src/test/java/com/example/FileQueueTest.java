package com.example;

import com.example.helpers.QueueServiceTester;
import org.junit.Test;

public class FileQueueTest {

    private final QueueServiceTester tester = new QueueServiceTester();
    private final String FLAVOR = DefaultQueueServiceFactory.FLAVOR_INTEGRATION;

    @Test
    public void pushHappyPathBasicFileTest() {

        tester.pushHappyPathBasicTest(FLAVOR);
    }

    @Test
    public void pushHappyPath3In2OutFileTest() {

        tester.pushHappyPath3In2OutTest(FLAVOR);
    }

    @Test
    public void pushHappyPath2In2OutFileTest() {

        tester.pushHappyPath2In2OutTest(FLAVOR);
    }

    @Test
    public void notDeletedMessagePutBackSuccessfullyFileTest() throws InterruptedException {

        tester.notDeletedMessagePutBackSuccessfullyTest(FLAVOR);
    }

    @Test
    public void multipleProducersInDifferentThreadsHappyPathFileTest() {

        tester.multipleProducersInDifferentThreadsHappyPathTest(FLAVOR);
    }

    @Test
    public void multipleConsumersInDifferentThreadsHappyPathFileTest() {

        tester.multipleConsumersInDifferentThreadsHappyPathTest(FLAVOR);
    }
}

