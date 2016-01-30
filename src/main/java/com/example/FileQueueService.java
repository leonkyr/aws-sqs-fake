package com.example;

import com.example.exceptions.QueueDoesNotExistException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileQueueService extends BaseLocalQueueService implements Closeable {

    private static final String NOT_SET_RECEIPT_HANDLE = "";
    private static final int DEFAULT_VISIBILITY_TIMEOUT_IN_SECONDS = 30;
    private static final boolean IS_NOT_TEMP_FILE = false;
    private static final boolean IS_TEMP_FILE = true;
    private static final String DATA_DIRECTORY = "sqs";
    private static final boolean APPEND_TO_FILE = true;

    public FileQueueService() {
        this(new MD5HashCalculator(), Executors.newScheduledThreadPool(10), new SimpleConsoleLogger());
    }

    // for DI
    public FileQueueService(
            HashCalculator hashCalculator,
            ScheduledExecutorService executorService,
            Logger logger) {

        super(hashCalculator, executorService, logger);
    }

    @Override
    public void push(String queueUrl, String messageBody) throws InterruptedException, IOException {
        getLogger().w("FILE push -> queueUrl = [" + queueUrl + "], messageBody = [" + messageBody + "]");

        validateQueueExists(queueUrl);

        File messagesFile = getMessageFile(queueUrl);
        File lock = getOrCreateLockFile(queueUrl);

        lock(lock);
        try (PrintWriter pw = new PrintWriter(new FileWriter(messagesFile, APPEND_TO_FILE))) {

            final DefaultMessage internalMessage =
                    DefaultMessage.create(
                            generateMessageId(),
                            NOT_SET_RECEIPT_HANDLE,
                            messageBody,
                            getHashCalculator().calculate(messageBody));

            pw.println(Record.create(internalMessage));

        } finally {
            unlock(lock);
        }
    }

    @Override
    public Message pull(String queueUrl, Integer visibilityTimeout) throws InterruptedException, IOException {
        getLogger().w("FILE pull -> queueUrl = [" + queueUrl + "], visibilityTimeout = [" + visibilityTimeout + "]");

        validateQueueExists(queueUrl);

        File messagesFile = getMessageFile(queueUrl);
        File messagesTemporaryFile = getMessageFile(queueUrl, IS_TEMP_FILE);
        File lock = getOrCreateLockFile(queueUrl);

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

                resultMessage = processRecordOnPull(queueUrl, visibilityTimeout, internalMessage, resultMessage, writer, line);
            }
        }
        finally {
            final boolean renameResult = messagesTemporaryFile.renameTo(messagesFile);
            getLogger().w("Renamed Successfully? " + renameResult);
            unlock(lock);
        }

        return resultMessage;
    }

    private DefaultMessage processRecordOnPull(String queueUrl, Integer visibilityTimeout, DefaultMessage internalMessage, DefaultMessage resultMessage, PrintWriter writer, String line) {
        // we cannot pull message with receiptHandle
        if (resultMessage == null &&
                internalMessage.getReceiptHandle().equals("")) {

            final long timeout = setVisibilityTimeoutToMessage(visibilityTimeout, internalMessage);
            final String receiptHandle = setReceiptHandleForMessage(internalMessage);

            resultMessage = internalMessage;

            String updatedRecord = Record.create(internalMessage);
            // write the updated record
            writer.println(updatedRecord);

            scheduleMessageDeleteVerification(queueUrl, timeout, receiptHandle);
        }
        else {
            // let's write the same record.. no changes
            writer.println(line);
        }
        return resultMessage;
    }

    private void scheduleMessageDeleteVerification(String queueUrl, long timeout, String receiptHandle) {
        getScheduledExecutorService()
            .schedule(
                    () -> {
                        DefaultMessage message = null;
                        try {
                            message = findMessageByReceiptHandle(receiptHandle, queueUrl);

                            // message was NOT deleted
                            if (message != null) {
                                // let me put it back at the head
                                readdMessageToQueueAfterTimeoutedPull(queueUrl, message);
                            }
                        } catch (InterruptedException | IOException e) {
                            e.printStackTrace();
                            getLogger().w(e.getStackTrace());
                        }
                    },
                    timeout, TimeUnit.SECONDS);
    }

    @Override
    public Message pull(String queueUrl) throws InterruptedException, IOException {
        return pull(queueUrl, DEFAULT_VISIBILITY_TIMEOUT_IN_SECONDS);
    }

    @Override
    public void delete(String queueUrl, String receiptHandle)
            throws InterruptedException, IOException{
        getLogger().w("FILE delete -> queueUrl = [" + queueUrl + "], receiptHandle = [" + receiptHandle + "]");

        validateQueueExists(queueUrl);

        File messagesFile = getMessageFile(queueUrl);
        File messagesTemporaryFile = getMessageFile(queueUrl, IS_TEMP_FILE);
        File lock = getOrCreateLockFile(queueUrl);

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
        String messagesFilePath = String.format("%s/%s/messages", DATA_DIRECTORY, queueName);

        try {
            createFile(messagesFilePath);
        } catch (IOException e) {
            // let's ignore it for now, BUT I need better idea what should be done here
            e.printStackTrace();
        }

        return queueName;
    }

    @Override
    public void deleteQueue(String queueUrl) {
        String messagesFilePath = String.format("%s/%s/messages", DATA_DIRECTORY, queueUrl);
        String parentDirectory = String.format("%s/%s", DATA_DIRECTORY, queueUrl);

        try {
            Files.delete(Paths.get(messagesFilePath));
            Files.delete(Paths.get(parentDirectory));
        } catch (IOException e) {
            // we need to be silent if we cannot remove the queue
            e.printStackTrace();
            getLogger().w(e.getStackTrace());
        }
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

    private void validateQueueExists(String queueUrl) {

        String messagesFilePath = String.format("%s/%s/messages", DATA_DIRECTORY, queueUrl);

        File targetFile = new File(messagesFilePath);
        File parent = targetFile.getParentFile();

        if (queueUrl == null ||
                queueUrl.isEmpty() ||
                !parent.exists()) {
            throw new QueueDoesNotExistException("Queue " + queueUrl + " does not exist.");
        }
    }

    private File getOrCreateLockFile(String queueName) throws IOException {
        String lockFilePath = String.format("%s/%s/.lock", DATA_DIRECTORY, queueName);

        getLogger().w("lockFilePath = " + lockFilePath);

        return createFile(lockFilePath);
    }

    private File getMessageFile(String queueUrl) {
        return getMessageFile(queueUrl, IS_NOT_TEMP_FILE);
    }

    private File getMessageFile(String queueUrl, boolean isTemporary) {
        String fileName = isTemporary ? UUID.randomUUID().toString() : "messages";

        String messagesFilePath = String.format("%s/%s/%s", DATA_DIRECTORY, queueUrl, fileName);
        getLogger().w("messagesFilePath = " + messagesFilePath);

        return new File(messagesFilePath);
    }

    private File createFile(String path) throws IOException {

        File targetFile = new File(path);
        File parent = targetFile.getParentFile();
        if(!parent.exists() && !parent.mkdirs()){
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }

        return targetFile;
    }

    private void readdMessageToQueueAfterTimeoutedPull(String queueName, DefaultMessage message)
            throws InterruptedException, IOException {

        File messagesFile = getMessageFile(queueName, IS_NOT_TEMP_FILE);
        File messagesTemporaryFile = getMessageFile(queueName, IS_TEMP_FILE);
        File lock = getOrCreateLockFile(queueName);

        lock(lock);
        getLogger().w("Before READD");
        printFileContent(messagesFile);
        try
        {
            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
                 PrintWriter writer = new PrintWriter(new FileWriter(messagesTemporaryFile, APPEND_TO_FILE))) {

                String handleToDelete = writeNotDeletedMessage(message, writer);

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

    private String writeNotDeletedMessage(DefaultMessage message, PrintWriter writer) {
        message.incrementRequeueCount();
        String handleToDelete = message.getReceiptHandle();
        message.setReceiptHandle("");

        String updatedRecord = Record.create(message);
        // let's write not deleted message first - aka FIFO
        writer.println(updatedRecord);
        return handleToDelete;
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

    private DefaultMessage findMessageByReceiptHandle(
            final String receiptHandle,
            final String queueUrl) throws InterruptedException, IOException {

        File messagesFile = getMessageFile(queueUrl, IS_NOT_TEMP_FILE);
        File lock = getOrCreateLockFile(queueUrl);

        lock(lock);

        DefaultMessage messageToDelete = null;
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

        return messageToDelete;
    }
}