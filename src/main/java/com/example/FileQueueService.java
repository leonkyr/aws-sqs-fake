package com.example;

import java.io.IOException;

public class FileQueueService implements QueueService {

    @Override
    public void push(String queueName, String message) throws InterruptedException, IOException {

    }

    @Override
    public Message pull(String queueName, Integer visibilityTimeout) throws InterruptedException, IOException {
        return null;
    }

    @Override
    public Message pull(String queueName) throws InterruptedException, IOException {
        return null;
    }

    @Override
    public void delete(String queueName, String receiptHandle) {

    }

//    private void lock(File lock) throws InterruptedException {
//        while (!lock.mkdir()) {
//            Thread.sleep(50);
//        }
//    }
//
//    private void unlock(File lock) {
//        lock.delete();
//    }
//
//    @Override
//    public void push(String queueUrl, Integer delaySeconds, String... messages)
//            throws InterruptedException, IOException {
//        String queue = fromUrl(queueUrl);
//        File messagesFile = getMessagesFile(queue);
//        File lock = getLockFile(queue);
//        long visibileFrom = (delaySeconds != null) ? new Date().getTime() + TimeUnit.SECONDS.toMillis(delaySeconds) : 0L;
//
//        lock(lock);
//        try (PrintWriter pw = new PrintWriter(new FileWriter(messagesFile, true))) {  // append
//            for (String message : messages) {
//                pw.println(Record.create(visibileFrom, message));
//            }
//        } finally {
//            unlock(lock);
//        }
//    }
//
//    private File getLockFile(String queue) {
//        return null;
//    }
//
//    private File getMessagesFile(String queue) {
//        return null;
//    }
//
//    private String fromUrl(String queueUrl) {
//        return null;
//    }
}
