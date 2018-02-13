package com.example;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class Message<T> implements Delayed, Serializable {

  private static final long TIMEOUT_MILLISECONDS = 30_000L;

  private final int attempts;
  private final long visibleFrom;
  private final T body;
  private final String receiptHandle;
  private final Clock clock;

  public Message(int attempts, long visibleFrom, String receiptHandle, T body, Clock clock) {
    this.attempts = attempts;
    this.visibleFrom = visibleFrom;
    this.receiptHandle = receiptHandle;
    this.body = body;
    this.clock = clock;
  }

  public int getAttempts() {
    return attempts;
  }

  public long getVisibleFrom() {
    return visibleFrom;
  }

  public String getReceiptHandle() {
    return receiptHandle;
  }

  public T getBody() {
    return body;
  }

  public static <T> Message<T> from(T body, Clock clock) {
    return new Message<>(0, 0, "", body, clock);
  }

  public static <T> Message<T> fromOld(Message<T> message) {
    return Message.<T>builder()
        .attempts(message.attempts + 1)
        .visibleFrom(message.now() + TIMEOUT_MILLISECONDS)
        .receiptHandle(randomUUID().toString())
        .body(message.body)
        .clock(message.clock)
        .build();
  }

  private boolean isVisible() {
    return visibleFrom <= now();
  }

  private long now() {
    return Instant.now(clock).toEpochMilli();
  }

  public static boolean isVisible(String message, Clock clock) {
    return fromString(message, clock).isVisible();
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(visibleFrom - now(), MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed other) {
    return Long.compare(visibleFrom, other.getDelay(MILLISECONDS));
  }

  public static <T> Message<T> withReceiptHandle(String receiptHandle) {
    return Message.<T>builder().receiptHandle(receiptHandle).build();
  }

  public static <T> Builder<T> builder() {
    return new Builder<T>();
  }

  public static final class Builder<T> {

    private int attempts;
    private long visibleFrom;
    private T body;
    private String receiptHandle;
    private Clock clock;

    public Builder<T> attempts(int attempts) {
      this.attempts = attempts;
      return this;
    }

    public Builder<T> visibleFrom(long visibleFrom) {
      this.visibleFrom = visibleFrom;
      return this;
    }

    public Builder<T> body(T body) {
      this.body = body;
      return this;
    }

    public Builder<T> receiptHandle(String receiptHandle) {
      this.receiptHandle = receiptHandle;
      return this;
    }

    public Builder<T> clock(Clock clock) {
      this.clock = clock;
      return this;
    }

    public Message<T> build() {
      return new Message<>(attempts, visibleFrom, receiptHandle, body, clock);
    }

  }

  @Override
  public String toString() {
    return attempts + ":" + visibleFrom + ":" + receiptHandle + ":" + body;
  }

  public static Message<String> fromString(String message, Clock clock) {
    String[] parts = message.split(":", 4);
    return new Message<>(parseInt(parts[0]), parseLong(parts[1]), parts[2], new String(Base64.getDecoder().decode(parts[3])), clock);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Message message = (Message) o;
    return Objects.equals(receiptHandle, message.receiptHandle);
  }

  @Override
  public int hashCode() {
    return Objects.hash(receiptHandle);
  }
}
