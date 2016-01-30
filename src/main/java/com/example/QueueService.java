package com.example;

import java.io.IOException;

public interface QueueService {

    /**
     * Gets or creates a queue and returns queue url
     * @param queueName
     * @return queue url
     */
    String createQueue(String queueName);

    /**
     * Deletes an existing queue if queue exists or does nothing
     * @param queueUrl
     */
    void deleteQueue(String queueUrl);

    /**
     * Pushes method to the queue with @queueUrl
     * @param queueUrl is the name of the queue
     * @param messageBody is the body of the message, JSON is welcome
     * @throws InterruptedException
     * @throws IOException
     */
    void push(String queueUrl, String messageBody)
            throws InterruptedException, IOException;

    /**
     * Pulls a message from the queue with @queueUrl and visibility timeout.
     * If the pulled message won't be deleted inside the visibility timeout, the message will be returned to the
     * beginning of the queue
     * @param queueUrl
     * @param visibilityTimeout
     * @return A pulled message
     * @throws InterruptedException
     * @throws IOException
     */
    Message pull(String queueUrl, Integer visibilityTimeout)
            throws InterruptedException, IOException;

    /**
     * Pulls a message from the queue with @queueUrl.
     * If the pulled message won't be deleted inside the DEFAULT visibility timeout, the message will be returned to the
     * beginning of the queue
     * @param queueUrl
     * @return A pulled message
     * @throws InterruptedException
     * @throws IOException
     */
    Message pull(String queueUrl)
            throws InterruptedException, IOException;

    /**
     * Deletes a message from the queue by @receiptHandle which is returned when the message is pulled
     * @param queueUrl
     * @param receiptHandle
     * @throws InterruptedException
     * @throws IOException
     */
    void delete(String queueUrl, String receiptHandle)
            throws InterruptedException, IOException;
}

