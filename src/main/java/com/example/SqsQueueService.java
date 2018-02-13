package com.example;

import com.amazonaws.services.sqs.AmazonSQS;

import java.util.Optional;

import static java.lang.Integer.parseInt;

public class SqsQueueService implements QueueService<String> {

  static final String APPROXIMATE_RECEIVE_COUNT = "ApproximateReceiveCount";

  private final AmazonSQS sqsClient;

  public SqsQueueService(AmazonSQS sqsClient) {
    this.sqsClient = sqsClient;
  }

  @Override
  public void push(String queueName, String messageBody) {
    createQueueIfNeeded(queueName);

    sqsClient.sendMessage(toUrl(queueName), messageBody);
  }

  @Override
  public Optional<Message<String>> pull(String queueName) {
    createQueueIfNeeded(queueName);

    return sqsClient.receiveMessage(toUrl(queueName))
        .getMessages()
        .stream()
        .findFirst()
        .map(this::sqsMessageToMessage);
  }

  private Message<String> sqsMessageToMessage(com.amazonaws.services.sqs.model.Message sqsMessage) {
    return Message.<String>builder()
        .receiptHandle(sqsMessage.getReceiptHandle())
        .body(sqsMessage.getBody())
        .attempts(parseInt(sqsMessage.getAttributes().get(APPROXIMATE_RECEIVE_COUNT)))
        .build();
  }

  @Override
  public void delete(String queueName, String receiptHandle) {
    createQueueIfNeeded(queueName);

    sqsClient.deleteMessage(toUrl(queueName), receiptHandle);
  }

  private void createQueueIfNeeded(String queueName) {
    sqsClient.createQueue(queueName);
  }

  private String toUrl(String queueName) {
    return sqsClient.getQueueUrl(queueName).getQueueUrl();
  }
}
