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
     * </p>
     */
    private long visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT_IN_MILLS;
    private int requeueCount;

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

    public long getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public void setReceiptHandle(String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }

    public void setVisibilityTimeout(long visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    public int getRequeueCount() {
        return requeueCount;
    }

    public void incrementRequeueCount() {
        this.requeueCount++;
    }

    public static DefaultMessage create(String messageId, String receiptHandle, String body){
        return new DefaultMessage(messageId, receiptHandle, body, "");
    }

    public static DefaultMessage create(String messageId, String receiptHandle, String body, String MD5OfBody){
        return new DefaultMessage(messageId, receiptHandle, body, MD5OfBody);
    }

    @Override
    public String toString() {
        return "DefaultMessage{" +
                "messageId='" + messageId + '\'' +
                ", receiptHandle='" + receiptHandle + '\'' +
                ", body='" + body + '\'' +
                ", MD5OfBody='" + MD5OfBody + '\'' +
                ", visibilityTimeout=" + visibilityTimeout +
                '}';
    }
}
