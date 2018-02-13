package com.example;

import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class QueueTestBase<T extends QueueService<String>> {

  protected Clock clock;

  protected T queueService;

  protected String someQueue;
  protected String someMessage;
  protected String someReceiptHandle;

  protected abstract T newQueueService();


  @Before
  public void baseSetUp() {
    someQueue = randomUUID().toString();
    someMessage = randomUUID().toString() + "\nfoo";
    someReceiptHandle = randomUUID().toString();

    clock = mock(Clock.class);
    setTimeTo(0L);
    queueService = newQueueService();
  }

  @Test(expected = IllegalArgumentException.class)
  public void pushHandlesNullInputs() {
    queueService.push(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void pushHandlesNullQueueName() {
    queueService.push(null, someMessage);
  }

  @Test(expected = IllegalArgumentException.class)
  public void pushHandlesEmptyQueueName() {
    queueService.push("", someMessage);
  }

  @Test(expected = IllegalArgumentException.class)
  public void pushHandlesNullMessage() {
    queueService.push(someQueue, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void pullHandlesNullInput() {
    queueService.pull(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void pullHandlesEmptyInput() {
    queueService.pull("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteHandlesNullInputs() {
    queueService.delete(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteHandlesNullQueueName() {
    queueService.delete(null, someReceiptHandle);
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteHandlesEmptyQueueName() {
    queueService.delete("", someReceiptHandle);
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteHandlesNullReceiptHandle() {
    queueService.delete(someQueue, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteHandlesEmptyReceiptHandle() {
    queueService.delete(someQueue, "");
  }

  @Test
  public void canPushMessageToQueue() {
    queueService.push(someQueue, someMessage);
  }

  @Test
  public void canPullMessagesFromQueue() {
    queueService.push(someQueue, someMessage);

    Optional<Message<String>> message = queueService.pull(someQueue);

    assertThat(message.isPresent(), is(true));
    assertThat(message.get().getBody(), is(equalTo(someMessage)));
    assertThat(message.get().getReceiptHandle(), is(any(String.class)));
    assertThat(message.get().getAttempts(), is(1));
    assertThat(message.get().getVisibleFrom(), is(30_000L));
    assertThat(message.get().getDelay(MILLISECONDS), is(30_000L));
  }

  @Test
  public void canDeleteMessagesFromQueue() {
    queueService.push(someQueue, someMessage);
    Optional<Message<String>> message = queueService.pull(someQueue);

    queueService.delete(someQueue, message.get().getReceiptHandle());

    assertThat(queueService.pull(someQueue).isPresent(), is(false));
  }

  @Test
  public void canPullMessagesFromEmptyQueue() {
    Optional<Message<String>> message = queueService.pull(someQueue);

    assertThat(message.isPresent(), is(false));
  }

  @Test
  public void canDeleteMessagesFromEmptyQueueWithoutExceptions() {
    queueService.delete(someQueue, someReceiptHandle);
  }

  @Test
  public void queueWorksAsFifo() {
    String someOtherMessage = randomUUID().toString();
    queueService.push(someQueue, someMessage);
    queueService.push(someQueue, someOtherMessage);

    Optional<Message<String>> firstMessage = queueService.pull(someQueue);
    Optional<Message<String>> secondMessage = queueService.pull(someQueue);


    assertThat(firstMessage.get().getBody(), is(someMessage));
    assertThat(secondMessage.get().getBody(), is(someOtherMessage));
  }

  @Test
  public void messagesDontBecomeVisibleBeforeVisibilityTimeout() {
    queueService.push(someQueue, someMessage);
    queueService.pull(someQueue);
    setTimeTo(29_999L);

    Optional<Message<String>> message = queueService.pull(someQueue);

    assertThat(message.isPresent(), is(false));
  }

  @Test
  public void canPullMessageAgainOnVisibilityTimeout() {
    queueService.push(someQueue, someMessage);
    queueService.pull(someQueue);
    setTimeTo(30_000L);

    Optional<Message<String>> message = queueService.pull(someQueue);

    assertThat(message.isPresent(), is(true));
    assertThat(message.get().getBody(), is(someMessage));
  }

  @Test
  public void canPullMessageAgainAfterVisibilityTimeout() {
    queueService.push(someQueue, someMessage);
    queueService.pull(someQueue);
    setTimeTo(30_001L);

    Optional<Message<String>> message = queueService.pull(someQueue);

    assertThat(message.isPresent(), is(true));
    assertThat(message.get().getBody(), is(someMessage));
  }

  private void setTimeTo(long milliseconds) {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(milliseconds));
  }

}