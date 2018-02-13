package com.example;

import org.junit.Test;

import java.time.Clock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MessageTest {

  private final Clock clock = Clock.systemUTC();

  private final Message message = Message.builder()
      .attempts(1)
      .visibleFrom(123)
      .receiptHandle("hash")
      .body("message")
      .clock(clock)
      .build();

  private final String messageString = "1:123:hash:message";

  @Test
  public void toStringWorksCorrectly() {
    assertThat(message.toString(), is(equalTo(messageString)));
  }

  @Test
  public void fromStringWorksCorrectly() {
    Message fromString = Message.fromString(messageString, clock);

    assertThat(fromString.getAttempts(), is(equalTo(message.getAttempts())));
    assertThat(fromString.getVisibleFrom(), is(equalTo(message.getVisibleFrom())));
    assertThat(fromString.getReceiptHandle(), is(equalTo(message.getReceiptHandle())));
    assertThat(fromString.getBody(), is(equalTo(message.getBody())));
    assertThat(fromString, is(equalTo(message)));
  }

}