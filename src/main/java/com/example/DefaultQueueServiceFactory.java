package com.example;

class DefaultQueueServiceFactory implements QueueServiceFactory {

    @Override
    public QueueService create(String flavor) {
        // flavor probably not the best selector for factory
        // but after reading the article I would say it is OK one
        QueueService result;

        switch (flavor){
            case "local": result = new InMemoryQueueService();
                break;
            default:
                throw new IllegalArgumentException(String.format("flavor[=%s] is not supported", flavor));
        }
        return result;
    }
}
