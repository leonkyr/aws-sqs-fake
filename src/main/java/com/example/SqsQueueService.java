package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageResult;

import java.io.IOException;

public class SqsQueueService implements QueueService {
    private final AmazonSQSClient sqsClient;
    private final Logger logger;

    // for DI
    public SqsQueueService(AmazonSQSClient sqsClient, Logger logger) {
        this.sqsClient = sqsClient;
        this.logger = logger;
    }

    public SqsQueueService(){
        this(AmazonSqsClientFactory.create(), new SimpleConsoleLogger());
    }

    public AmazonSQSClient getSqsClient() {
        return sqsClient;
    }

    @Override
    public void push(String queueName, String messageBody) throws InterruptedException, IOException {
        // null pointer checks will be done later
        String queueUrl = getSqsClient().getQueueUrl(queueName).getQueueUrl();

        final SendMessageResult sendMessageResult = getSqsClient().sendMessage(queueUrl, messageBody);

        logger.w(sendMessageResult.toString());
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