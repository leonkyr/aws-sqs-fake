package com.example;

public class SimpleConsoleLogger implements Logger {
    @Override
    public void w(Object objectToLog) {
        System.out.println(objectToLog);
    }
}
