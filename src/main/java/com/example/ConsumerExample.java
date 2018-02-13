package com.example;

public class ConsumerExample {

  /*
  constructor: number of workers in the pool that is going to do the processing of these messages,
  queueservice,
  java.util.FunctionConsumer,
  String queueName

  what it's going to do:
  start() method
  when it's called it's going to build a thread pool
  it's going pulling messages from the queue
  pass it to the threadpool
  workers in the thread pool going to pass the message to the
   */

  /*
  poll the queue
   */

//  public ConsumerExample(int numberOfWorkers,
//                         QueueService<String> queueService,
//                         Consumer<String> consumer,
//                         String queueName) {
//    this.numberOfWorkers = numberOfWorkers;
//    this.queueService = queueService;
//    this.consumer = consumer;
//    this.queueName = queueName;
//  }
//
//  public void start() {
//    ThreadPool threadPool = new ThreadPool(numberOfWorkers, consumer);
//
//    while (true) {
//      Message message = queueService.pull();
//      threadPool.consume(message);
//    }
//
//  }
}
