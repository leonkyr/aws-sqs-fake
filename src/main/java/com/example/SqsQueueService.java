package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;

import java.io.IOException;

public class SqsQueueService implements QueueService {
    private final AmazonSQSClient sqsClient;

    public SqsQueueService(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    @Override
    public void push(String queueName, String message) throws InterruptedException, IOException {

    }

    @Override
    public Message pull(String queueName, Integer visibilityTimeout) throws InterruptedException, IOException {
        return null;
    }

    @Override
    public Message pull(String queueName) throws InterruptedException, IOException {
        return null;
    }

    @Override
    public void delete(String queueName, String receiptHandle) {

    }
}
