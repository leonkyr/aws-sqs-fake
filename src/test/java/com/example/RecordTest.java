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

        final DefaultMessage parsedMessage = Record.parse(record);

        Assert.assertTrue("Parsed message is not equal to original message", message.equals(parsedMessage));
    }
}
