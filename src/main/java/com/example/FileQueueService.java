package com.example;

import java.io.*;
import java.util.UUID;
import java.util.concurrent.*;

public class FileQueueService extends BaseLocalQueueService implements Closeable {

    private static final int ZERO_REQUEUE_COUNT = 0;
    private static final String NOT_SET_RECEIPT_HANDLE = "";
    private static final int DEFAULT_VISIBILITY_TIMEOUT_IN_SECONDS = 5;
    private static final boolean IS_NOT_TEMP_FILE = false;
    private static final boolean IS_TEMP_FILE = true;
    private static final long DEFAULT_RETRY_TIMEOUT_IN_MILLS = 1000;

    private final Logger logger;
    private final HashCalculator hashCalculator;
    private final String DATA_DIRECTORY = "sqs";

    private final ExecutorService executorService;

    public FileQueueService() {
        this(new MD5HashCalculator(), new SimpleConsoleLogger());
    }

    // for DI
    public FileQueueService(HashCalculator hashCalculator, Logger logger) {

        this.hashCalculator = hashCalculator;
        this.logger = logger;

        this.executorService = Executors.newSingleThreadExecutor();
    }

    public HashCalculator getHashCalculator() {
        return hashCalculator;
    }

    @Override
    public void push(String queueName, String messageBody) throws InterruptedException, IOException {
        logger.w("queueName = [" + queueName + "], messageBody = [" + messageBody + "]");

        File messagesFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
        File lock = getLockFile(queueName);
        final String hash = getHashCalculator().calculate(messageBody);

        lock(lock);
        logger.w("Before PUT");
        printFileContent(messagesFile);
        try (PrintWriter pw = new PrintWriter(new FileWriter(messagesFile, true))) {  // append
            pw.println(Record.create(
                    ZERO_REQUEUE_COUNT,
                    NOT_SET_RECEIPT_HANDLE,
                    messageBody,
                    hash,
                    DEFAULT_VISIBILITY_TIMEOUT_IN_SECONDS));
        } finally {
            unlock(lock);
        }
        logger.w("AFTER PUT");
        printFileContent(messagesFile);
    }

    @Override
    public Message pull(String queueName, Integer visibilityTimeout) throws InterruptedException, IOException {
        logger.w("queueName = [" + queueName + "], visibilityTimeout = [" + visibilityTimeout + "]");

        File messagesFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
        File messagesTemporaryFile = getMessageFile(queueName, IS_TEMP_FILE);
        File lock = getLockFile(queueName);

        lock(lock);
        logger.w("Before PULL");
        printFileContent(messagesFile);
        DefaultMessage internalMessage,
                resultMessage = null;
        try
        {
            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
                 PrintWriter writer = new PrintWriter(new FileWriter(messagesTemporaryFile, true))) {  // append

                for(String line; (line = reader.readLine()) != null;) {

                    internalMessage = Record.parse(line);
                    // we don't have messages in the file anymore
                    if (internalMessage == null)
                        return resultMessage;

                    // we cannot pull message with receiptHandle
                    if (resultMessage == null &&
                            internalMessage.getReceiptHandle().equals("")) {
                        final long timeout = setVisibilityTimeoutToMessage(visibilityTimeout, internalMessage);
                        final String receiptHandle = setReceiptHandleForMessage(internalMessage);

                        String updatedRecord = Record.create(internalMessage);
                        writer.println(updatedRecord);
                        resultMessage = internalMessage;

                        Future<?> task = executorService.submit(() -> {
                            try {
                                verifyVisibilityTimeoutOnDelete(receiptHandle, queueName);
                            } catch (InterruptedException e) {
                                // we will handle later properly
                                e.printStackTrace();
                            }
                        });

                        waitTillTaskExecutedAsync(queueName, internalMessage, timeout, task);
                    }
                    else {
                        // let's write file to the end
                        writer.println(line);
                    }
                }
            }
        }
        finally {
            final boolean renameResult = messagesTemporaryFile.renameTo(messagesFile);
            logger.w("Renamed Successfully? " + renameResult);
            unlock(lock);
        }

        logger.w("AFTER PULL");
        printFileContent(messagesFile);

        return resultMessage;
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
        logger.w("Before DELETE");
        printFileContent(messagesFile);
        try
        {
            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
                 PrintWriter writer = new PrintWriter(new FileWriter(messagesTemporaryFile, true))) {  // append

                boolean deleted = false;
                for (String line; (line = reader.readLine()) != null;) {
                    if (!deleted) {
                        Message internalMessage = Record.parse(line);

                        logger.w(internalMessage);

                        if (internalMessage != null &&
                                internalMessage.getReceiptHandle().equals(receiptHandle)) {
                            logger.w("DELETED ---_> " + line);
                            deleted = true;
                            continue;
                        }
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

        logger.w("After DELETE");
        printFileContent(messagesFile);
    }

    private void printFileContent(File file) {
        try {
            logger.w("-----" + file.getAbsolutePath() + "------");
            try (BufferedReader reader = new BufferedReader(new FileReader(file))){
                for (String line; (line = reader.readLine()) != null; ) {
                    logger.w(line);
                }
            }
            logger.w("-----------");
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    private void waitTillTaskExecutedAsync(
            final String queueName,
            final DefaultMessage message,
            final long timeout,
            Future<?> task) {

        // simple way to do async wait..
        new Thread(() -> {
            try {
                task.get(timeout, TimeUnit.MILLISECONDS);
                logger.w("Message was deleted");
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
                task.cancel(true);

                // handle exception better later
                try {
                    // re-add message at the beginning
                    readdMessageToQueueAfterTimeoutedPull(queueName, message);
                } catch (InterruptedException | IOException ex) {
                    ex.printStackTrace();
                }

                logger.w("Message has been put back because it was not deleted.");
            }
        }).start();

    }

    private void readdMessageToQueueAfterTimeoutedPull(String queueName, DefaultMessage message)
            throws InterruptedException, IOException {

        File messagesFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
        File messagesTemporaryFile = getMessageFile(queueName, IS_TEMP_FILE);
        File lock = getLockFile(queueName);

        lock(lock);
        logger.w("Before READD");
        printFileContent(messagesFile);
        try
        {
            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
                 PrintWriter writer = new PrintWriter(new FileWriter(messagesTemporaryFile, true))) {  // append

                message.incrementRequeueCount();
                message.setReceiptHandle("");

                String updatedRecord = Record.create(message);
                writer.println(updatedRecord);

                for (String line; (line = reader.readLine()) != null;) {
                    DefaultMessage internalMessage = Record.parse(line);

                    if (internalMessage != null &&
                            internalMessage.getReceiptHandle().equals(message.getReceiptHandle())) {
                        // we wrote the message before reading the file
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

        logger.w("After READD");
        printFileContent(messagesFile);
    }

    private void verifyVisibilityTimeoutOnDelete(
            final String receiptHandle,
            final String queueName) throws InterruptedException {

        while (!Thread.currentThread().isInterrupted()) {

            File messagesFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
            File lock = getLockFile(queueName);

            lock(lock);
            logger.w("Before DELETE");
            printFileContent(messagesFile);

            Message messageToDelete = null;
            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile))) {  // append

                for (String line; (line = reader.readLine()) != null;) {
                    messageToDelete = Record.parse(line);

                    if (messageToDelete != null &&
                            messageToDelete.getReceiptHandle().equals(receiptHandle)) {
                        // the message is still here
                        break;
                    }
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            finally {
                unlock(lock);
            }

            // message was delete
            if (messageToDelete != null) {
                // let's wait and check again
                try {
                    Thread.sleep(DEFAULT_RETRY_TIMEOUT_IN_MILLS);
                } catch (InterruptedException e) {
                    // I was asked to stop
                    return;
                }
            } else {
                logger.w("Message is null");
                return;
            }
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }
}