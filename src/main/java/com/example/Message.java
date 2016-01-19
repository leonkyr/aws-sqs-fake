package com.example;

public class Message {

    private final String messageId;
    private final String receiptHandle;
    private final String body;
    private final String MD5OfBody;


    private Message(String messageId, String receiptHandle, String body, String MD5OfBody) {
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

    public static Message create(String messageId, String receiptHandle, String body){
        return new Message(messageId, receiptHandle, body, "");
    }

    public static Message create(String messageId, String receiptHandle, String body, String MD5OfBody){
        return new Message(messageId, receiptHandle, body, MD5OfBody);
    }
}
