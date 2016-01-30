package com.example;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

public abstract class BaseLocalQueueService implements QueueService, Closeable {

    private final HashCalculator hashCalculator;
    private final Logger logger;
    private final ExecutorService executorService;
    private final MessageIdGenerator messageIdGenerator;

    // for DI
    public BaseLocalQueueService(
            HashCalculator hashCalculator,
            ExecutorService executorService,
            Logger logger) {

        if (hashCalculator == null) {
            throw new IllegalArgumentException("hashCalculator cannot be null.");
        }
        if (executorService == null) {
            throw new IllegalArgumentException("executorService cannot be null.");
        }
        if (logger == null) {
            throw new IllegalArgumentException("logger cannot be null.");
        }

        this.hashCalculator = hashCalculator;
        this.logger = logger;
        this.executorService = executorService;
        this.messageIdGenerator = new SecureMessageIdGenerator(); // Dependency Injection later
    }

    protected HashCalculator getHashCalculator() {
        return hashCalculator;
    }

    protected Logger getLogger() {
        return logger;
    }

    protected ExecutorService getExecutorService() {
        return executorService;
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

    @Override
    public void close() throws IOException {
        getExecutorService().shutdown();
    }
}

