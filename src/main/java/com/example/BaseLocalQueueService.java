package com.example;

import com.example.exceptions.QueueDoesNotExistException;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

public abstract class BaseLocalQueueService implements QueueService, Closeable {

    private static final Integer DEFAULT_VISIBILITY_TIMEOUT = 30;
    private static final String NOT_SET_RECEIPT_HANDLE = "";

    private final HashCalculator hashCalculator;
    private final Logger logger;
    private final ScheduledExecutorService scheduledExecutorService;
    private final MessageIdGenerator messageIdGenerator;

    // for DI
    public BaseLocalQueueService(
            HashCalculator hashCalculator,
            ScheduledExecutorService scheduledExecutorService,
            Logger logger) {

        if (hashCalculator == null) {
            throw new IllegalArgumentException("hashCalculator cannot be null.");
        }
        if (scheduledExecutorService == null) {
            throw new IllegalArgumentException("scheduledExecutorService cannot be null.");
        }
        if (logger == null) {
            throw new IllegalArgumentException("logger cannot be null.");
        }

        this.hashCalculator = hashCalculator;
        this.logger = logger;
        this.scheduledExecutorService = scheduledExecutorService;
        this.messageIdGenerator = new SecureMessageIdGenerator(); // Dependency Injection later
    }

    @Override
    public void close() throws IOException {
        getScheduledExecutorService().shutdown();
    }

    @Override
    public String createQueue(String queueName) {
        getLogger().w("queueName = [" + queueName + "]");

        return doCreateQueue(queueName);
    }

    protected abstract String doCreateQueue(String queueName);

    @Override
    public void deleteQueue(String queueUrl) {
        getLogger().w("queueUrl = [" + queueUrl + "]");

        doDeleteQueue(queueUrl);
    }

    protected abstract void doDeleteQueue(String queueUrl);

    @Override
    public void push(String queueUrl, String messageBody) throws InterruptedException, IOException {

        getLogger().w("push -> queueName = [" + queueUrl + "], messageBoy = [" + messageBody + "]");

        validateQueue(queueUrl);

        final DefaultMessage internalMessage =
                DefaultMessage.create(
                        generateMessageId(),
                        NOT_SET_RECEIPT_HANDLE,
                        messageBody,
                        getHashCalculator().calculate(messageBody));

        doPush(queueUrl, internalMessage);
    }

    protected abstract void doPush(String queueUrl, DefaultMessage internalMessage)
            throws IOException, InterruptedException;

    @Override
    public Message pull(String queueUrl) throws InterruptedException, IOException {
        return pull(queueUrl, DEFAULT_VISIBILITY_TIMEOUT);
    }

    @Override
    public Message pull(String queueUrl, Integer visibilityTimeout) throws InterruptedException, IOException {
        getLogger().w("queueUrl = [" + queueUrl + "], visibilityTimeout = [" + visibilityTimeout + "]");

        validateQueue(queueUrl);

        return doPull(queueUrl, visibilityTimeout);
    }

    protected abstract Message doPull(String queueUrl, Integer visibilityTimeout)
            throws InterruptedException, IOException;

    @Override
    public void delete(String queueUrl, String receiptHandle)
            throws InterruptedException, IOException {

        getLogger().w("queueUrl = [" + queueUrl + "], receiptHandle = [" + receiptHandle + "]");

        validateQueue(queueUrl);

        doDelete(queueUrl, receiptHandle);
    }

    protected abstract void doDelete(String queueUrl, String receiptHandle)
            throws InterruptedException, IOException;

    protected abstract void validateQueue(String queueUrl) throws QueueDoesNotExistException;

    protected HashCalculator getHashCalculator() {
        return hashCalculator;
    }

    protected Logger getLogger() {
        return logger;
    }

    protected ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    private MessageIdGenerator getMessageIdGenerator() {
        return messageIdGenerator;
    }

    protected String setReceiptHandleForMessage(DefaultMessage internalMessage) {
        internalMessage.setReceiptHandle(internalMessage.generateReceiptHandle());
        return internalMessage.getReceiptHandle();
    }

    protected long setVisibilityTimeoutToMessage(Integer visibilityTimeout, DefaultMessage internalMessage) {
        final long timeout = this.calculateVisibility(visibilityTimeout);
        internalMessage.setVisibilityTimeout(timeout);
        return timeout;
    }

    protected String generateMessageId(){
        return getMessageIdGenerator().generateMessageId();
    }

    private  long calculateVisibility(Integer visibilityTimeoutInSeconds) {
        return visibilityTimeoutInSeconds * 1000;
    }
}

