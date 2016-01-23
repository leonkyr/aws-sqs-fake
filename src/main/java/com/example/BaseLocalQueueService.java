package com.example;

public abstract class BaseLocalQueueService implements QueueService {
    protected String setReceiptHandleForMessage(DefaultMessage internalMessage) {
        internalMessage.setReceiptHandle(internalMessage.generateReceiptHandle());
        return internalMessage.getReceiptHandle();
    }

    protected long setVisibilityTimeoutToMessage(Integer visibilityTimeout, DefaultMessage internalMessage) {
        final long timeout = this.calculateVisibility(visibilityTimeout);
        internalMessage.setVisibilityTimeout(timeout);
        return timeout;
    }

    private  long calculateVisibility(Integer visibilityTimeoutInSeconds) {
        return visibilityTimeoutInSeconds * 1000;
    }
}
