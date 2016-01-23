package com.example;

import java.io.IOException;

public interface QueueService {

    /**
     * Pushes method to the queue with @queueName
     * @param queueName is the name of the queue
     * @param messageBody is the body of the message, JSON is welcome
     * @throws InterruptedException
     * @throws IOException
     */
    void push(String queueName, String messageBody)
            throws InterruptedException, IOException;

    /**
     * Pulls a message from the queue with @queueName and visibility timeout.
     * If the pulled message won't be deleted inside the visibility timeout, the message will be returned to the
     * beginning of the queue
     * @param queueName
     * @param visibilityTimeout
     * @return A pulled message
     * @throws InterruptedException
     * @throws IOException
     */
    Message pull(String queueName, Integer visibilityTimeout)
            throws InterruptedException, IOException;

    /**
     * Pulls a message from the queue with @queueName.
     * If the pulled message won't be deleted inside the DEFAULT visibility timeout, the message will be returned to the
     * beginning of the queue
     * @param queueName
     * @return A pulled message
     * @throws InterruptedException
     * @throws IOException
     */
    Message pull(String queueName)
            throws InterruptedException, IOException;

    /**
     * Deletes a message from the queue by @receiptHandle which is returned when the message is pulled
     * @param queueName
     * @param receiptHandle
     * @throws InterruptedException
     * @throws IOException
     */
    void delete(String queueName, String receiptHandle)
            throws InterruptedException, IOException;
}

