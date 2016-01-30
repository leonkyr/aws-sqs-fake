package com.example;

import com.example.helpers.MessageGenerator;
import junit.framework.Assert;
import org.junit.Test;

import java.util.UUID;

public class RecordTest {

    @Test
    public void SerializeAndDeserialize(){
        String messageBody = new MessageGenerator().generate();

        final DefaultMessage message =
                DefaultMessage.create(
                        UUID.randomUUID().toString(),
                        "",
                        messageBody,
                        UUID.randomUUID().toString());

        String record = Record.create(message);

        System.out.println();
        System.out.println("record = " + record);

        final DefaultMessage parsedMessage = Record.parse(record);

        System.out.println();
        System.out.println("parsedMessage = " + parsedMessage);

        Assert.assertTrue(message.equals(parsedMessage));
    }
}
