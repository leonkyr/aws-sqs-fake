package com.example.helpers;

import java.util.UUID;

public class QueueNameGenerator {
    public String generate(){
        return UUID.randomUUID().toString();
    }
}
