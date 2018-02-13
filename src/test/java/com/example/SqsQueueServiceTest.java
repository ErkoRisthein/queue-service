package com.example;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner.StrictStubs;

import java.util.Collections;
import java.util.Optional;

import static com.example.SqsQueueService.APPROXIMATE_RECEIVE_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(StrictStubs.class)
public class SqsQueueServiceTest {

  @Mock
  private AmazonSQS sqsClient;

  private QueueService<String> queueService;

  private static final String someQueue = "someQueue";
  private static final String someMessage = "someMessage";
  private static final String someUrl = "someUrl";
  private static final String someBody = "someBody";
  private static final String someReceiptHandle = "someReceiptHandle";

  @Before
  public void setUp() {
    queueService = new SqsQueueService(sqsClient);
  }

  @Test
  public void canPushMessagesToQueue() {
    mockQueueUrl(someQueue, someUrl);

    queueService.push(someQueue, someMessage);

    verify(sqsClient).createQueue(someQueue);
    verify(sqsClient).sendMessage(someUrl, someMessage);
  }

  @Test
  public void canPullMessagesFromQueue() {
    mockQueueUrl(someQueue, someUrl);
    when(sqsClient.receiveMessage(someUrl)).thenReturn(receiveMessageResultWith(someReceiptHandle, someBody));

    Optional<Message<String>> message = queueService.pull(someQueue);

    assertThat(message.isPresent(), is(true));
    assertThat(message.get().getBody(), is(someBody));
    assertThat(message.get().getReceiptHandle(), is(someReceiptHandle));
    assertThat(message.get().getAttempts(), is(1));
  }

  @Test
  public void canDeleteMessagesFromQueue() {
    mockQueueUrl(someQueue, someUrl);

    queueService.delete(someQueue, someReceiptHandle);

    verify(sqsClient).deleteMessage(someUrl, someReceiptHandle);
  }

  private void mockQueueUrl(String someQueue, String someUrl) {
    when(sqsClient.getQueueUrl(someQueue)).thenReturn(new GetQueueUrlResult().withQueueUrl(someUrl));
  }

  private ReceiveMessageResult receiveMessageResultWith(String receiptHandle, String body) {
    return new ReceiveMessageResult()
      .withMessages(new com.amazonaws.services.sqs.model.Message()
        .withReceiptHandle(receiptHandle)
        .withBody(body)
      .withAttributes(Collections.singletonMap(APPROXIMATE_RECEIVE_COUNT, "1")));
  }

}