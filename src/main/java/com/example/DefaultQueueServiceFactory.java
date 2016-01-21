package com.example;

public class DefaultQueueServiceFactory implements QueueServiceFactory {

    public static final String FLAVOR_LOCAL = "local";
    public static final String FLAVOR_INTEGRATION = "integration";
    public static final String FLAVOR_PRODUCTION = "production";

    @Override
    public QueueService create(String flavor) {
        // flavor probably not the best selector for factory
        // but after reading the article I would say it is OK one
        QueueService result;

        switch (flavor){
            case FLAVOR_LOCAL:
                result = new InMemoryQueueService();
                break;
            case FLAVOR_INTEGRATION:
                result = new FileQueueService();
                break;
            case FLAVOR_PRODUCTION:
                result = new SqsQueueService();
                break;
            default:
                throw new IllegalArgumentException(String.format("flavor[=%s] is not supported", flavor));
        }
        return result;
    }
}
