package com.example;

import java.math.BigInteger;
import java.security.SecureRandom;

public class MessageGenerator {
    private SecureRandom random = new SecureRandom();

    public String generate() {
        return "{ id : \"" + new BigInteger(130, random).toString(32) + "\"}";
    }
}
