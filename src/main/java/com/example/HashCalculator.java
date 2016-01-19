package com.example;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public interface HashCalculator {
    String calculate(String input);
}

