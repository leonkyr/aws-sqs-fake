package com.example;

import com.example.helpers.QueueServiceTester;
import org.junit.Test;

public class InMemoryQueueTest {

    private final QueueServiceTester tester = new QueueServiceTester();
    private final String FLAVOR = DefaultQueueServiceFactory.FLAVOR_LOCAL;


    @Test
    public void pushHappyPathBasicInMemTest() {

        tester.pushHappyPathBasicTest(FLAVOR);
    }

    @Test
    public void pushHappyPath3In2OutInMemTest() {

        tester.pushHappyPath3In2OutTest(FLAVOR);
    }

    @Test
    public void pushHappyPath2In2OutInMemTest() {

        tester.pushHappyPath2In2OutTest(FLAVOR);
    }

    @Test
    public void putThrowsWithoutQueueInMemTest(){
        tester.putThrowsWithoutQueueTest(FLAVOR);
    }

    @Test
    public void pullThrowsWithoutQueueInMemTest(){
        tester.pullThrowsWithoutQueueTest(FLAVOR);
    }

    @Test
    public void deleteThrowsWithoutQueueInMemTest(){
        tester.deleteThrowsWithoutQueueTest(FLAVOR);
    }

    @Test
    public void notDeletedMessagePutBackSuccessfullyInMemTest() throws InterruptedException {

        tester.notDeletedMessagePutBackSuccessfullyTest(FLAVOR);
    }

    @Test
    public void multipleProducersInDifferentThreadsHappyPathInMemTest() {

        tester.multipleProducersInDifferentThreadsHappyPathTest(FLAVOR);
    }

    @Test
    public void multipleConsumersInDifferentThreadsHappyPathInMemTest() {

        tester.multipleConsumersInDifferentThreadsHappyPathTest(FLAVOR);
    }
}
