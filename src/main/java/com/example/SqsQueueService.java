package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import java.io.IOException;

public class SqsQueueService implements QueueService {

    private static final Integer DEFAULT_VISIBILITY_TIMEOUT = 30;

    private final AmazonSQSClient sqsClient;
    private final Logger logger;

    public SqsQueueService(){
        this(AmazonSqsClientFactory.create(), new SimpleConsoleLogger());
    }

    // for DI
    public SqsQueueService(AmazonSQSClient sqsClient, Logger logger) {
        if (sqsClient == null) {
            throw new IllegalArgumentException("sqsClient cannot be null.");
        }
        if (logger == null) {
            throw new IllegalArgumentException("logger cannot be null.");
        }

        this.sqsClient = sqsClient;
        this.logger = logger;
    }


    public AmazonSQSClient getSqsClient() {
        return sqsClient;
    }

    @Override
    public void push(String queueUrl, String messageBody)
            throws InterruptedException, IOException {

        logger.w("SQS push -> queueUrl = [" + queueUrl + "], messageBody = [" + messageBody + "]");

        final SendMessageResult sendMessageResult =
                getSqsClient()
                    .sendMessage(queueUrl, messageBody);

        logger.w(sendMessageResult.toString());
    }

    @Override
    public Message pull(String queueUrl, Integer visibilityTimeout)
            throws InterruptedException, IOException {

        logger.w("SQS pull -> queueUrl = [" + queueUrl + "], visibilityTimeout = [" + visibilityTimeout + "]");

        ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest(visibilityTimeout, queueUrl);

        final ReceiveMessageResult receiveMessageResult = getSqsClient().receiveMessage(receiveMessageRequest);

        Message result = null;
        if (!receiveMessageResult.getMessages().isEmpty()) {
            result = convert(receiveMessageResult.getMessages().get(0));
        }

        return result;
    }

    @Override
    public Message pull(String queueUrl)
            throws InterruptedException, IOException {

        return pull(queueUrl, DEFAULT_VISIBILITY_TIMEOUT);
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) {
        logger.w("SQS delete -> queueUrl = [" + queueUrl + "], receiptHandle = [" + receiptHandle + "]");

        getSqsClient().deleteMessage(queueUrl, receiptHandle);
    }

    @Override
    public String createQueue(String queueName) {
        final CreateQueueResult createQueueResult = getSqsClient().createQueue(queueName);

        return createQueueResult.getQueueUrl();
    }

    @Override
    public void deleteQueue(String queueUrl) {

        getSqsClient().deleteQueue(queueUrl);
    }

    private ReceiveMessageRequest createReceiveMessageRequest(Integer visibilityTimeout, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
        receiveMessageRequest.setVisibilityTimeout(visibilityTimeout);
        receiveMessageRequest.setQueueUrl(queueUrl);
        return receiveMessageRequest;
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