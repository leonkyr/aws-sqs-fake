package com.example;

import java.io.*;
import java.util.UUID;
import java.util.concurrent.*;

public class FileQueueService extends BaseLocalQueueService implements Closeable {

    private static final int ZERO_REQUEUE_COUNT = 0;
    private static final String NOT_SET_RECEIPT_HANDLE = "";
    private static final int DEFAULT_VISIBILITY_TIMEOUT_IN_SECONDS = 30;
    private static final boolean IS_NOT_TEMP_FILE = false;
    private static final boolean IS_TEMP_FILE = true;
    private static final long DEFAULT_RETRY_TIMEOUT_IN_MILLS = 1000;
    private static final String DATA_DIRECTORY = "sqs";

    public FileQueueService() {
        this(new MD5HashCalculator(), Executors.newSingleThreadExecutor(), new SimpleConsoleLogger());
    }

    // for DI
    public FileQueueService(
            HashCalculator hashCalculator,
            ExecutorService executorService,
            Logger logger) {

        super(hashCalculator, executorService, logger);
    }

    @Override
    public void push(String queueUrl, String messageBody) throws InterruptedException, IOException {
        getLogger().w("FILE push -> queueName = [" + queueUrl + "], messageBody = [" + messageBody + "]");

        File messagesFile = getMessageFile(queueUrl, IS_NOT_TEMP_FILE);
        File lock = getLockFile(queueUrl);
        final String hash = getHashCalculator().calculate(messageBody);

        lock(lock);
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
    }

    @Override
    public Message pull(String queueUrl, Integer visibilityTimeout) throws InterruptedException, IOException {
        getLogger().w("FILE pull -> queueName = [" + queueUrl + "], visibilityTimeout = [" + visibilityTimeout + "]");

        File messagesFile = getMessageFile(queueUrl, IS_NOT_TEMP_FILE);
        File messagesTemporaryFile = getMessageFile(queueUrl, IS_TEMP_FILE);
        File lock = getLockFile(queueUrl);

        lock(lock);
        DefaultMessage internalMessage,
                resultMessage = null;

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

                    resultMessage =
                            getMessageFromQueueAndScheduleVisibilityCheck(queueUrl, visibilityTimeout, internalMessage, writer);
                }
                else {
                    // let's write file to the end
                    writer.println(line);
                }
            }
        }
        finally {
            final boolean renameResult = messagesTemporaryFile.renameTo(messagesFile);
            getLogger().w("Renamed Successfully? " + renameResult);
            unlock(lock);
        }

        return resultMessage;
    }

    @Override
    public Message pull(String queueUrl) throws InterruptedException, IOException {
        return pull(queueUrl, DEFAULT_VISIBILITY_TIMEOUT_IN_SECONDS);
    }

    @Override
    public void delete(String queueUrl, String receiptHandle)
            throws InterruptedException, IOException{
        getLogger().w("FILE delete -> queueName = [" + queueUrl + "], receiptHandle = [" + receiptHandle + "]");

        File messagesFile = getMessageFile(queueUrl, IS_NOT_TEMP_FILE);
        File messagesTemporaryFile = getMessageFile(queueUrl, IS_TEMP_FILE);
        File lock = getLockFile(queueUrl);

        lock(lock);
        try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
             PrintWriter writer = new PrintWriter(new FileWriter(messagesTemporaryFile, true))) {  // append

            boolean deleted = false;
            for (String line; (line = reader.readLine()) != null;) {
                if (!deleted) {
                    Message internalMessage = Record.parse(line);

                    if (internalMessage != null &&
                            internalMessage.getReceiptHandle().equals(receiptHandle)) {
                        getLogger().w("DELETED ----> " + line);
                        deleted = true;
                        continue;
                    }
                }

                writer.println(line);
            }
        }
        finally {
            final boolean renameResult = messagesTemporaryFile.renameTo(messagesFile);
            getLogger().w("Renamed Successfully? " + renameResult);
            unlock(lock);
        }
    }

    @Override
    public String createQueue(String queueName) {
        return null;
    }

    @Override
    public void deleteQueue(String queueUrl) {

    }

    private DefaultMessage getMessageFromQueueAndScheduleVisibilityCheck(
            String queueName,
            Integer visibilityTimeout,
            DefaultMessage internalMessage,
            PrintWriter writer) {

        DefaultMessage resultMessage;

        final long timeout = setVisibilityTimeoutToMessage(visibilityTimeout, internalMessage);
        final String receiptHandle = setReceiptHandleForMessage(internalMessage);

        resultMessage = internalMessage;

        String updatedRecord = Record.create(internalMessage);
        writer.println(updatedRecord);

        Future<?> task = getExecutorService().submit(() -> {
            try {
                verifyVisibilityTimeoutOnDelete(receiptHandle, queueName);
            } catch (InterruptedException e) {
                // we will handle later properly
                e.printStackTrace();
            }
        });

        waitTillTaskExecutedAsync(queueName, internalMessage, timeout, task);
        return resultMessage;
    }

    private void printFileContent(File file) {
        try {
            getLogger().w("-----" + file.getAbsolutePath() + "------");
            try (BufferedReader reader = new BufferedReader(new FileReader(file))){
                for (String line; (line = reader.readLine()) != null; ) {
                    getLogger().w(line);
                }
            }
            getLogger().w("-----------");
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
        // TODO: Refactor bad error handling
        new Thread(() -> {
            try {
                task.get(timeout, TimeUnit.MILLISECONDS);
                getLogger().w("Message was deleted");
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

                getLogger().w("Message has been put back because it was not deleted.");
            }
        }).start();

    }

    private void readdMessageToQueueAfterTimeoutedPull(String queueName, DefaultMessage message)
            throws InterruptedException, IOException {

        File messagesFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
        File messagesTemporaryFile = getMessageFile(queueName, IS_TEMP_FILE);
        File lock = getLockFile(queueName);

        lock(lock);
        getLogger().w("Before READD");
        printFileContent(messagesFile);
        try
        {
            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
                 PrintWriter writer = new PrintWriter(new FileWriter(messagesTemporaryFile, true))) {  // append

                message.incrementRequeueCount();
                String handleToDelete = message.getReceiptHandle();
                message.setReceiptHandle("");

                String updatedRecord = Record.create(message);
                // let's write not deleted message first - aka FIFO
                writer.println(updatedRecord);

                writeFileToEndExceptReceiptHandle(reader, writer, handleToDelete);
            }
        }
        finally {
            final boolean renameResult = messagesTemporaryFile.renameTo(messagesFile);
            getLogger().w("Renamed Successfully? " + renameResult);
            unlock(lock);
        }

        getLogger().w("After READD");
        printFileContent(messagesFile);
    }

    private void writeFileToEndExceptReceiptHandle(BufferedReader reader, PrintWriter writer, String handleToDelete) throws IOException {
        for (String line; (line = reader.readLine()) != null;) {
            Message msg = Record.parse(line);

            if (msg != null &&
                    msg.getReceiptHandle().equals(handleToDelete)) {
                // we wrote the message before reading the file
                continue;
            }

            writer.println(line);
        }
    }

    private void verifyVisibilityTimeoutOnDelete(
            final String receiptHandle,
            final String queueName) throws InterruptedException {

        while (!Thread.currentThread().isInterrupted()) {

            File messagesFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
            File lock = getLockFile(queueName);

            lock(lock);

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
                getLogger().w("Message is null");
                return;
            }
        }
    }

    @Override
    public void close() throws IOException {
        getExecutorService().shutdown();
    }
}