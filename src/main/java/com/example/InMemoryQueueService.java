package com.example;

import java.io.IOException;

public class InMemoryQueueService implements QueueService {
    @Override
    public void push(String queueUrl, Integer delay, String message) throws InterruptedException, IOException {

    }

    @Override
    public String pull(String queueName) throws InterruptedException, IOException {
        return null;
    }

    @Override
    public void delete(String message) {

    }
}

