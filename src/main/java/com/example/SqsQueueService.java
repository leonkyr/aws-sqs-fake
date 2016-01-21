package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import java.io.IOException;

public class SqsQueueService implements QueueService {

    private static final Integer DEFAULT_VISIBILITY_TIMEOUT = 30;

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
    public void push(String queueName, String messageBody)
            throws InterruptedException, IOException {

        logger.w("SQS push -> queueName = [" + queueName + "], messageBody = [" + messageBody + "]");

        String queueUrl = getQueueUrl(queueName);

        final SendMessageResult sendMessageResult =
                getSqsClient()
                    .sendMessage(queueUrl, messageBody);

        logger.w(sendMessageResult.toString());
    }

    @Override
    public Message pull(String queueName, Integer visibilityTimeout)
            throws InterruptedException, IOException {

        logger.w("SQS pull -> queueName = [" + queueName + "], visibilityTimeout = [" + visibilityTimeout + "]");

        String queueUrl = getQueueUrl(queueName);

        ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest(visibilityTimeout, queueUrl);

        final ReceiveMessageResult receiveMessageResult = getSqsClient().receiveMessage(receiveMessageRequest);

        Message result = null;
        if (!receiveMessageResult.getMessages().isEmpty()) {
            result = convert(receiveMessageResult.getMessages().get(0));
        }

        return result;
    }

    @Override
    public Message pull(String queueName)
            throws InterruptedException, IOException {

        return pull(queueName, DEFAULT_VISIBILITY_TIMEOUT);
    }

    @Override
    public void delete(String queueName, String receiptHandle) {
        logger.w("SQS delete -> queueName = [" + queueName + "], receiptHandle = [" + receiptHandle + "]");

        String queueUrl = getQueueUrl(queueName);

        getSqsClient().deleteMessage(queueUrl, receiptHandle);
    }

    private ReceiveMessageRequest createReceiveMessageRequest(Integer visibilityTimeout, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
        receiveMessageRequest.setVisibilityTimeout(visibilityTimeout);
        receiveMessageRequest.setQueueUrl(queueUrl);
        return receiveMessageRequest;
    }

    private String getQueueUrl(String queueName) {
        final CreateQueueResult createQueueResult = getSqsClient().createQueue(queueName);

        return createQueueResult.getQueueUrl();
    }

    private Message convert(com.amazonaws.services.sqs.model.Message message) {

        if (message == null)
            return null;

        return DefaultMessage.create(
                message.getMessageId(),
                message.getReceiptHandle(),
                message.getBody(),
                message.getMD5OfBody());
    }
}