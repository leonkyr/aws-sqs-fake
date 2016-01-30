package com.example;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

public abstract class BaseLocalQueueService implements QueueService, Closeable {

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

    @Override
    public void close() throws IOException {
        getScheduledExecutorService().shutdown();
    }
}

