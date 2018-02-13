package com.example;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import static com.google.common.base.Strings.isNullOrEmpty;

public class InMemoryQueueService implements QueueService<String> {

  private final Map<String, Queue<Message<String>>> queues = new ConcurrentHashMap<>();
  private final Clock clock;

  public InMemoryQueueService(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void push(String queueName, String messageBody) {
    if (isNullOrEmpty(queueName) || messageBody == null) {
      throw new IllegalArgumentException();
    }
    Queue<Message<String>> queue = getQueue(queueName);
    queue.add(Message.from(messageBody, clock));
  }

  private Queue<Message<String>> getQueue(String queueName) {
    return queues.computeIfAbsent(queueName, n -> new DelayQueue<>());
  }

  @Override
  public Optional<Message<String>> pull(String queueName) {
    if (isNullOrEmpty(queueName)) {
      throw new IllegalArgumentException();
    }

    Queue<Message<String>> queue = getQueue(queueName);

    Optional<Message<String>> oldMessage = Optional.ofNullable(queue.poll());
    Optional<Message<String>> newMessage = oldMessage.map(Message::fromOld);
    newMessage.ifPresent(queue::add);

    return newMessage;
  }


  @Override
  public void delete(String queueName, String receiptHandle) {
    if (isNullOrEmpty(queueName) || isNullOrEmpty(receiptHandle)) {
      throw new IllegalArgumentException();
    }

    Queue<Message<String>> queue = getQueue(queueName);
    queue.remove(Message.<String>withReceiptHandle(receiptHandle)); // O(n)
  }

}
