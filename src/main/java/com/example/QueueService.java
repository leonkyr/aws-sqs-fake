package com.example;

import java.io.IOException;

public interface QueueService {

    // 1. I could you Message that serialize and make a chain of inheritance.. but String is simple for the test

  //
  // Task 1: Define me.
  //
  // This interface should include the following methods.  You should choose appropriate
  // signatures for these methods that prioritise simplicity of implementation for the range of
  // intended implementations (in-memory, file, and SQS).  You may include additional methods if
  // you choose.
  //
  // - push
  //   pushes a message onto a queue.
  // - pull
  //   retrieves a single message from a queue.
  // - delete
  //   deletes a message from the queue that was received by pull().
  //

    void push(String queueUrl, Integer delay, String message)
            throws InterruptedException, IOException;

    String pull(String queueName)
            throws InterruptedException, IOException;

    void delete(String message);
}
