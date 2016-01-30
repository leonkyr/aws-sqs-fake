package com.example.helpers;

import java.math.BigInteger;
import java.security.SecureRandom;

public class MessageGenerator {
    private static final SecureRandom random;

    static {
        random = new SecureRandom();
    }

    public String generate() {
        String message = "{ \n" +
                    "id : \"" + new BigInteger(130, random).toString(32) + "\", \n" +
                    "\"key\": \"value\"\n" +
                "\"}";

        System.out.println("message = " + message);

        return message;
    }
}