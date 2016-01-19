package com.example;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5HashCalculator implements HashCalculator {
    @Override
    public String calculate(String input) {
        try {

            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(input.getBytes());
            byte[] digest = messageDigest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }

            String hash = sb.toString();

            System.out.println("original:" + messageDigest);
            System.out.println("digested(hex):" + hash);

            return hash;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }
}
