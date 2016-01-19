package com.example;

import org.junit.Assert;

public final class QueueTestGiven {
    private final QueueServiceFactory queueServiceFactory;
    private String queueName;
    private String flavor;

    public QueueTestGiven() {
        queueServiceFactory = new DefaultQueueServiceFactory();
    }

    public QueueTestGiven setEnvironmentFlavor(String flavor) {
        this.flavor = flavor;

        return this;
    }

    private String getQueueName() {
        return queueName;
    }

    private String getFlavor() {
        return flavor;
    }

    private QueueServiceFactory getQueueServiceFactory() {
        return queueServiceFactory;
    }

    public QueueTestWhen when() {
        QueueService queueService = getQueueServiceFactory()
                .create(getFlavor());

        Assert.assertNotNull(queueService);

        return new QueueTestWhen(
                queueService, getQueueName());
    }

    public QueueTestGiven and() {
        return this;
    }

    public QueueTestGiven setQueueName(String queueName) {
        this.queueName = queueName;

        return this;
    }
}
