package com.example;

import java.util.Optional;

public interface QueueService<T> {

  /**
   * Pushes a message onto a queue.
   * @param queueName the name of the queue
   * @param messageBody the message to push
   */
  void push(String queueName, T messageBody);

  /**
   * Retrieves a single message from a queue.
   * @param queueName the name of the queue
   * @return a message
   */
  Optional<Message<T>> pull(String queueName);

  /**
   * Deletes a message from the queue that was received by pull().
   * @param queueName the name of the queue
   * @param receiptHandle a unique receipt handle from Message.receiptHandle
   */
  void delete(String queueName, String receiptHandle);

}
