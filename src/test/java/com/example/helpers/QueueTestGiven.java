package com.example.helpers;

import com.example.DefaultQueueServiceFactory;
import com.example.QueueService;
import com.example.QueueServiceFactory;
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

        String queueUrl = getQueueName(); // have a queue name, but don't create it if it is empty

        if (getQueueName() != null &&
            !getQueueName().isEmpty()) {
            queueService.createQueue(getQueueName());
            System.out.println("Queue was not created.");
        }

        return new QueueTestWhen(
                queueService, queueUrl);
    }

    public QueueTestGiven and() {
        return this;
    }

    public QueueTestGiven setQueueName(String queueName) {
        this.queueName = queueName;

        return this;
    }
}
