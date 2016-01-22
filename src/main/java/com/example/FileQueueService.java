package com.example;

import java.io.*;
import java.util.UUID;

public class FileQueueService implements QueueService {

    private static final int ZERO_REQUEUE_COUNT = 0;
    private static final String NOT_SET_RECEIPT_HANDLE = "";
    private static final int DEFAULT_VISIBILITY_TIMEOUT_IN_SECONDS = 0;
    private static final boolean IS_NOT_TEMP_FILE = false;
    private static final boolean IS_TEMP_FILE = true;

    private final Logger logger;
    private final HashCalculator hashCalculator;
    private final String DATA_DIRECTORY = "sqs";

    public FileQueueService() {
        this(new MD5HashCalculator(), new SimpleConsoleLogger());
    }

    // for DI
    public FileQueueService(HashCalculator hashCalculator, Logger logger) {

        this.hashCalculator = hashCalculator;
        this.logger = logger;
    }

    public HashCalculator getHashCalculator() {
        return hashCalculator;
    }

    @Override
    public void push(String queueName, String messageBody) throws InterruptedException, IOException {
        logger.w("queueName = [" + queueName + "], messageBody = [" + messageBody + "]");

        File messageFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
        File lock = getLockFile(queueName);
        final String hash = getHashCalculator().calculate(messageBody);

        lock(lock);
        try (PrintWriter pw = new PrintWriter(new FileWriter(messageFile, true))) {  // append
            pw.println(Record.create(
                    ZERO_REQUEUE_COUNT,
                    NOT_SET_RECEIPT_HANDLE,
                    messageBody,
                    hash,
                    DEFAULT_VISIBILITY_TIMEOUT_IN_SECONDS));
        } finally {
            unlock(lock);
        }
    }

    @Override
    public Message pull(String queueName, Integer visibilityTimeout) throws InterruptedException, IOException {
        logger.w("queueName = [" + queueName + "], visibilityTimeout = [" + visibilityTimeout + "]");

        File messagesFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
        File messagesTemporaryFile = getMessageFile(queueName, IS_TEMP_FILE);
        File lock = getLockFile(queueName);

        lock(lock);
        try
        {
            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
                 PrintWriter writer = new PrintWriter(new FileWriter(messagesTemporaryFile, true))) {  // append
                String line = reader.readLine();

                DefaultMessage internalMessage = Record.parse(line);

                setVisibilityTimeoutToMessage(visibilityTimeout, internalMessage);
                setReceiptHandleForMessage(internalMessage);

                String updatedRecord = Record.create(internalMessage);

                writer.println(updatedRecord);
                while ((line = reader.readLine()) != null) {
                    writer.println(line);
                }

                return internalMessage;
            }
        }
        finally {
            final boolean renameResult = messagesTemporaryFile.renameTo(messagesFile);
            logger.w("Renamed Successfully? " + renameResult);
            unlock(lock);
        }
    }

    @Override
    public Message pull(String queueName) throws InterruptedException, IOException {
        return pull(queueName, 0);
    }

    @Override
    public void delete(String queueName, String receiptHandle)
            throws InterruptedException, IOException{
        logger.w("queueName = [" + queueName + "], receiptHandle = [" + receiptHandle + "]");

        File messagesFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
        File messagesTemporaryFile = getMessageFile(queueName, IS_TEMP_FILE);
        File lock = getLockFile(queueName);

        lock(lock);
        try
        {
            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
                 PrintWriter writer = new PrintWriter(new FileWriter(messagesTemporaryFile, true))) {  // append

                String line;
                while ((line = reader.readLine()) != null) {
                    Message internalMessage = Record.parse(line);

                    if (internalMessage != null &&
                            internalMessage.getReceiptHandle().equals(receiptHandle)) {
                        logger.w("DELETED ---_> " + line);
                        continue;
                    }

                    writer.println(line);
                }
            }
        }
        finally {
            final boolean renameResult = messagesTemporaryFile.renameTo(messagesFile);
            logger.w("Renamed Successfully? " + renameResult);
            unlock(lock);
        }
    }

    private String setReceiptHandleForMessage(DefaultMessage internalMessage) {
        internalMessage.setReceiptHandle();
        return internalMessage.getReceiptHandle();
    }

    private long setVisibilityTimeoutToMessage(Integer visibilityTimeout, DefaultMessage internalMessage) {
        final long timeout = this.calculateVisibility(visibilityTimeout);
        internalMessage.setVisibilityTimeout(timeout);
        return timeout;
    }

    private void lock(File lock) throws InterruptedException {
        while (!lock.mkdir()) {
            Thread.sleep(50);
        }
    }

    private void unlock(File lock) {
        lock.delete();
    }

    private File getLockFile(String queueName) {
        String lockFilePath = String.format("%s/%s/.lock", DATA_DIRECTORY, queueName);

        System.out.println("lockFilePath = " + lockFilePath);

        return createFile(lockFilePath);
    }

    private File getMessageFile(String queueName, boolean isTemporary) {
        String fileName = isTemporary ? UUID.randomUUID().toString() : "messages";

        String messagesFilePath = String.format("%s/%s/%s", DATA_DIRECTORY, queueName, fileName);
        System.out.println("messagesFilePath = " + messagesFilePath);

        return createFile(messagesFilePath);
    }

    private File createFile(String path) {

        File targetFile = new File(path);
        File parent = targetFile.getParentFile();
        if(!parent.exists() && !parent.mkdirs()){
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }

        return targetFile;
    }


}
