package com.example;

import java.math.BigInteger;
import java.security.SecureRandom;

public class SecureMessageIdGenerator implements MessageIdGenerator {
    private static final SecureRandom random;

    static {
        random = new SecureRandom();
    }

    @Override
    public String generateMessageId() {
        return new BigInteger(130, random).toString(32);
    }
}
