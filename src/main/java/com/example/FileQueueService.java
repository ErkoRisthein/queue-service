package com.example;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.example.Message.fromString;
import static com.example.Message.isVisible;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.UUID.randomUUID;

public class FileQueueService implements QueueService<String> {

  private final String queuesDirectory;
  private final Clock clock;

  public FileQueueService(String queuesDirectory, Clock clock) {
    this.queuesDirectory = queuesDirectory;
    this.clock = clock;
  }

  @Override
  public void push(String queueName, String messageBody) {
    if (isNullOrEmpty(queueName) || messageBody == null) {
      throw new IllegalArgumentException();
    }

    queueName = sanitize(queueName);
    messageBody = base64(messageBody);

    createQueueFiles(queueName);
    Path messages = getMessagesFile(queueName);
    Path lock = getLockFile(queueName);

    lock(lock);
    try (Writer messagesFile = newBufferedWriter(messages, CREATE, APPEND)) {
      Message<String> message = Message.from(messageBody, clock);
      messagesFile.write(message.toString());
      writeNewLine(messagesFile);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      unlock(lock);
    }
  }

  private String base64(String messageBody) {
    return Base64.getEncoder().encodeToString(messageBody.getBytes(Charset.defaultCharset()));
  }

  private Path getLockFile(String queueName) {
    return Paths.get(queuesDirectory, queueName, ".lock");
  }

  private Path getMessagesFile(String queueName) {
    return Paths.get(queuesDirectory, queueName, "messages");
  }

  private void createQueueFiles(String queueName) {
    try {
      Files.createDirectories(Paths.get(queuesDirectory, queueName));
      Files.createFile(getMessagesFile(queueName));
    } catch (FileAlreadyExistsException x) {
      // ignore
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void lock(Path lock) {
    while (!lock.toFile().mkdir()) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private void unlock(Path lock) {
    lock.toFile().delete();
  }

  @Override
  public Optional<Message<String>> pull(String queueName) {
    if (isNullOrEmpty(queueName)) {
      throw new IllegalArgumentException();
    }

    queueName = sanitize(queueName);

    createQueueFiles(queueName);
    Path messagesFile = getMessagesFile(queueName);
    Path lock = getLockFile(queueName);
    lock(lock);
    Path temporaryFile = getTemporaryFile(queueName);
    List<Message<String>> result = new ArrayList<>();

    try (Stream<String> lines = Files.lines(messagesFile);
         Writer tempFile = newBufferedWriter(temporaryFile, CREATE, APPEND)) {

      lines.forEachOrdered(line -> tryPullMessage(line, result, tempFile));

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      moveFile(temporaryFile, messagesFile);
      unlock(lock);
    }

    return result.stream().findFirst();
  }

  private String sanitize(String queueName) {
    HashFunction hashFunction = Hashing.md5();
    HashCode hashCode = hashFunction.hashString(queuesDirectory, Charset.defaultCharset());
    return hashCode.toString();
  }

  private void tryPullMessage(String line, List<Message<String>> result, Writer tempFile) {
    try {
      pullMessage(line, result, tempFile);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void tryDeleteMessage(String line, String receiptHandle, Writer tempFile) {
    try {
      deleteMessage(line, receiptHandle, tempFile);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void pullMessage(String message, List<Message<String>> result, Writer tempFile) throws IOException {
    if (isVisible(message, clock) && result.isEmpty()) {
      Message<String> oldMessage = fromString(message, clock);
      Message<String> newMessage = Message.fromOld(oldMessage);
      result.add(newMessage);
      tempFile.write(newMessage.toString());
    } else {
      tempFile.write(message);
    }
    writeNewLine(tempFile);
  }


  private void deleteMessage(String message, String receiptHandle, Writer tempFile) throws IOException {
    if (isVisible(message, clock) || !fromString(message, clock).equals(Message.<String>withReceiptHandle(receiptHandle))) {
      tempFile.write(message);
      writeNewLine(tempFile);
    }
  }

  private void moveFile(Path source, Path target) {
    try {
      Files.move(source, target, ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Path getTemporaryFile(String queueName) {
    return Paths.get(queuesDirectory, queueName, randomUUID().toString());
  }

  @Override
  public void delete(String queueName, String receiptHandle) {
    if (isNullOrEmpty(queueName) || isNullOrEmpty(receiptHandle)) {
      throw new IllegalArgumentException();
    }

    queueName = sanitize(queueName);

    createQueueFiles(queueName);
    Path messagesFile = getMessagesFile(queueName);
    Path lock = getLockFile(queueName);
    lock(lock);
    Path temporaryFile = getTemporaryFile(queueName);

    try (Stream<String> lines = Files.lines(messagesFile);
         Writer tempFile = newBufferedWriter(temporaryFile, CREATE, APPEND)) {

      lines.forEachOrdered(line -> tryDeleteMessage(line, receiptHandle, tempFile));

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      moveFile(temporaryFile, messagesFile);
      unlock(lock);
    }
  }

  private void writeNewLine(Writer writer) throws IOException {
    writer.write(System.getProperty("line.separator"));
  }

}
