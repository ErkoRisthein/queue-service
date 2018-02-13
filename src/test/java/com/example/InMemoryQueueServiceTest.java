package com.example;

public class InMemoryQueueServiceTest extends QueueTestBase<InMemoryQueueService> {

  @Override
  protected InMemoryQueueService newQueueService() {
    return new InMemoryQueueService(clock);
  }

}
