package com.example;

public class DefaultMessage implements Message {

    private static final long DEFAULT_VISIBILITY_TIMEOUT_IN_MILLS = 30 * 1000; // 30 seconds
    private final String messageId;
    private String receiptHandle;
    private final String body;
    private final String MD5OfBody;
    /**
     * <p>
     * The duration (in seconds) that the received messages are hidden from
     * subsequent retrieve requests after being retrieved by a
     * <code>ReceiveMessage</code> request.
     * </p>
     */
    private long visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT_IN_MILLS;

    private DefaultMessage(String messageId, String receiptHandle, String body, String MD5OfBody) {
        this.messageId = messageId;
        this.receiptHandle = receiptHandle;
        this.body = body;
        this.MD5OfBody = MD5OfBody;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public String getBody() {
        return body;
    }

    public String getMD5OfBody() {
        return MD5OfBody;
    }

    public void setReceiptHandle() {
        this.receiptHandle = generateReceiptHandle();
    }

    public void setVisibilityTimeout(long visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    public static DefaultMessage create(String messageId, String receiptHandle, String body){
        return new DefaultMessage(messageId, receiptHandle, body, "");
    }

    public static DefaultMessage create(String messageId, String receiptHandle, String body, String MD5OfBody){
        return new DefaultMessage(messageId, receiptHandle, body, MD5OfBody);
    }
}