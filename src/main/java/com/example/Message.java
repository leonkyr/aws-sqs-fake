package com.example;

import java.math.BigInteger;
import java.security.SecureRandom;

public interface Message {
    String getMessageId();

    String getReceiptHandle();

    String getBody();

    String getMD5OfBody();

    default String generateReceiptHandle(){
        SecureRandom random = new SecureRandom();

        return new BigInteger(130, random).toString(32);
    }
}