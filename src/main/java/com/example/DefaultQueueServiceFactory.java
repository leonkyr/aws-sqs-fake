package com.example;

class DefaultQueueServiceFactory implements QueueServiceFactory {

    @Override
    public QueueService create(String flavor) {
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
