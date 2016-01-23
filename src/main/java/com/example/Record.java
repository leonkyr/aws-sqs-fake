package com.example;

import java.util.Arrays;

public class Record {
    public final static int REQUEUE_COUNT_POSITION = 0;
    public final static int VISIBILITY_TIMEOUT_POSITION = 1;
    public final static int RECEIPT_HANDLE_POSITION = 2;
    public final static int MD5_HASH_POSITION = 3;
    public final static int MESSAGE_BODY_POSITION = 4;
    private static final String RECORD_DELIMITER = ":";

    // requeue count, visibility deadline, receipt id, and message contents.
    public static String create(
            int requeueCount,
            String receiptHandle,
            String messageBody,
            String hash,
            int visibilityTimeoutInSeconds) {

        return String.format("%d%s%d%s%s%s%s%s%s",
                requeueCount, RECORD_DELIMITER,
                visibilityTimeoutInSeconds, RECORD_DELIMITER,
                receiptHandle, RECORD_DELIMITER,
                hash, RECORD_DELIMITER,
                messageBody);
    }

    public static String create(DefaultMessage message) {
        if (message == null)
            return "";

        return create(
                message.getRequeueCount(),
                message.getReceiptHandle(),
                message.getBody(),
                message.getMD5OfBody(),
                (int)message.getVisibilityTimeout()/1000
        );
    }

    public static DefaultMessage parse(String line) {
        if (line == null || line.equals(""))
            return null;

        String[] words = line.split(RECORD_DELIMITER);
        final String[] bodyParts = Arrays.copyOfRange(words, 4, words.length);
        String messageBody = String.join(":", bodyParts);
        System.out.println("messageBody = " + messageBody);
        DefaultMessage result = DefaultMessage.create(
                "",
                words[RECEIPT_HANDLE_POSITION],
                messageBody,
                words[MD5_HASH_POSITION]
        );
        return result;
    }
}
