package com.example;

public final class QueueTestGiven {
    private final QueueServiceFactory queueServiceFactory;
    private QueueService queueService;
    private String queueName;

    public QueueTestGiven() {
        queueServiceFactory = new DefaultQueueServiceFactory();
    }

    public QueueTestGiven setEnvironmentFlavor(String flavor) {

        setQueueService(queueServiceFactory.create(flavor));

        return this;
    }

    private QueueService getQueueService() {
        return queueService;
    }

    public String getQueueName() {
        return queueName;
    }

    private void setQueueService(QueueService queueService) {
        this.queueService = queueService;
    }

    public QueueTestWhen when() {
        return new QueueTestWhen(
                getQueueService(), getQueueName());
    }

    public QueueTestGiven and() {
        return this;
    }

    public QueueTestGiven setQueueName(String queueName) {
        this.queueName = queueName;

        return this;
    }
}
