package com.example;

import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.Comparator.reverseOrder;
import static java.util.UUID.randomUUID;

public class FileQueueServiceTest extends QueueTestBase<FileQueueService> {

  private static final String queuesDirectory = randomUUID().toString();

  @Override
  protected FileQueueService newQueueService() {
    return new FileQueueService(queuesDirectory, clock);
  }

  @After
  public void tearDown() throws IOException {
    Path queues = Paths.get(queuesDirectory);
    if(queues.toFile().exists()) {
      Files.walk(queues)
          .sorted(reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

}
