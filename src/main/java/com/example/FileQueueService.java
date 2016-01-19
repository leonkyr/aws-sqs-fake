//package com.example;
//
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.Date;
//import java.util.concurrent.TimeUnit;
//
//public class FileQueueService implements QueueService {
//  //
//  // Task 3: Implement me if you have time.
//  //
//
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
//}
