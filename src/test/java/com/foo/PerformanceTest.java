package com.foo;

import com.foo.request.GetRes;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.gson.Gson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.netty.DisposableServer;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 curl -v -H "Content-Type: application/json" -d '{"owner":"john", "topic":"android"}' localhost:8080/v1/topic/register
 curl -v -H "Content-Type: application/json" -d '{"owner":"john", "topic":"android", "msg":"pixel3"}' localhost:8080/v1/message/publish
 curl -v -H "Content-Type: application/json" -d '{"user":"nico", "topic":"android"}' localhost:8080/v1/topic/subscribe
 curl -v -H "Content-Type: application/json" -d '{"user":"nico", "topic":"android"}' localhost:8080/v1/message/get
 curl -v -H "Content-Type: application/json" -d '{"owner":"john", "topic":"android", "msg":"galaxy23"}' localhost:8080/v1/message/publish
 curl -v -H "Content-Type: application/json" -d '{"user":"nico", "topic":"android", "msgIdx":"2"}' localhost:8080/v1/message/ack
*/
public class PerformanceTest {
  static final Jedis jedis = new Jedis("localhost");
  final String host = "localhost";
  final int port = 8080;
  final int maxMsgSize = 128 * 1024;
  final int maxNameLen = 100;
  final int maxTopics = 1;
  final int maxSubscribers = 5;
  final HttpRequestFactory reqFactory = (new NetHttpTransport()).createRequestFactory();
  final Gson gson = new Gson();

  @BeforeEach
  void flushAll() {
    jedis.flushAll();
  }

  boolean register(String owner, String topic) {
    String req = String.format("{\"owner\": \"%s\", \"topic\": \"%s\"}", owner, topic);
    try {
      HttpResponse res = reqFactory.buildPostRequest(
        new GenericUrl("http://localhost:8080/v1/topic/register"),
        ByteArrayContent.fromString("application/json", req)
      ).execute();
      System.out.println(res.toString());
      return res.isSuccessStatusCode();

    } catch(IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  boolean publish(String owner, String topic, String msg) {
    String req = String.format("{\"owner\": \"%s\", \"topic\": \"%s\", \"msg\": \"%s\"}", owner, topic, msg);
    try {
      HttpResponse res = reqFactory.buildPostRequest(
        new GenericUrl("http://localhost:8080/v1/message/publish"),
        ByteArrayContent.fromString("application/json", req)
      ).execute();
      return res.isSuccessStatusCode();

    } catch(IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  boolean subscribe(String subscriber, String topic) {
    String req = String.format("{\"subscriber\": \"%s\", \"topic\": \"%s\"}", subscriber, topic);
    try {
      HttpResponse res = reqFactory.buildPostRequest(
        new GenericUrl("http://localhost:8080/v1/topic/subscribe"),
        ByteArrayContent.fromString("application/json", req)
      ).execute();
      return res.isSuccessStatusCode();

    } catch(IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  GetRes getMessage(String subscriber, String topic) {
    String req = String.format("{\"subscriber\": \"%s\", \"topic\": \"%s\"}", subscriber, topic);
    try {
      System.out.println("===============> Sending get req");
      HttpResponse res = reqFactory.buildPostRequest(
        new GenericUrl("http://localhost:8080/v1/message/get"),
        ByteArrayContent.fromString("application/json", req)
      ).execute();

      if (res.isSuccessStatusCode()) {
        try(Reader isr = new InputStreamReader(res.getContent())) {
          GetRes getRes = gson.fromJson(isr, GetRes.class);
          System.out.println("===============> GetRes: " + getRes);
          return getRes;
        }
      } else {
        return null;
      }
    } catch(IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  boolean ackMessage(String subscriber, String topic, long msgIdx) {
    String req = String.format("{\"subscriber\": \"%s\", \"topic\": \"%s\", \"msgIdx\": \"%d\"}", subscriber, topic, msgIdx);
    try {
      HttpResponse res = reqFactory.buildPostRequest(
        new GenericUrl("http://localhost:8080/v1/message/ack"),
        ByteArrayContent.fromString("application/json", req)
      ).execute();
      return res.isSuccessStatusCode();

    } catch(IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  void runTest(
    int maxQueueSize,
    int numMsgs,
    ExecutorService exec,
    CountDownLatch clientFinished,
    Runnable test
  ) {
    CountDownLatch serverReady = new CountDownLatch(1);
    CountDownLatch serverShutdown = new CountDownLatch(1);

    Cfg cfg = new Cfg(
      "localhost", 8080, maxMsgSize, maxQueueSize, maxNameLen, maxTopics, maxSubscribers);
    MessageQueues mq = new RedisMessageQueues(cfg);

    exec.submit(() -> {
      DisposableServer server = new Application(mq).buildHttpServer(host, port);
      System.out.println("Started MQ server");
      serverReady.countDown();
      try {
        clientFinished.await();
      } catch(InterruptedException ex) {
        System.err.printf("Interrupted while waiting the server to shutdown: %s%n", ex);
      }
      server.disposeNow();
      System.out.println("Shut down MQ server");
      serverShutdown.countDown();
    });

    long beg = System.currentTimeMillis();
    try {
      serverReady.await();
      System.out.println("Started testing");

      test.run();
      clientFinished.await();

      long time = System.currentTimeMillis() - beg;
      double rpms = ((double) time) / numMsgs;
      System.out.printf("%nTook %d ms%n", time);
      System.out.printf("# of msgs: %d%n", numMsgs);
      System.out.printf("%f req/ms; %d req/sec %n", rpms, (int) (rpms * 1000));

      serverShutdown.await();

    } catch(Exception ex) {
      System.err.println(ex.toString());
    }
  }

  Runnable buildPublisher(
    String owner,
    String topic,
    int numMsgs,
    CountDownLatch clientFinished,
    CountDownLatch registeredTopic
  ) {
    return () -> {
      while (!register(owner, topic)) { System.out.println("Registering..."); }
      registeredTopic.countDown();
      int msgLeft = numMsgs;

      while(msgLeft > 0) {
        String msg = String.format("msg-%d", numMsgs - msgLeft + 1);
        if (publish(owner, topic, msg)) {
          msgLeft--;
        }
      }
      clientFinished.countDown();
    };
  }

  Runnable buildConsumer(
    String subscriber,
    String topic,
    int numMsgs,
    CountDownLatch clientFinished,
    CountDownLatch registeredTopic
  ) {
    return () -> {
      try { registeredTopic.await(); } catch(InterruptedException ex) { /**/ }

      if (!subscribe(subscriber, topic)) {
        throw new IllegalStateException("Failed to subscribe to topic");
      }
      int msgLeft = numMsgs;
      boolean firstMsg = true;

      while(msgLeft > 0) {
        try {
          GetRes getRes = getMessage(subscriber, topic);
          if (getRes != null) {
            if (firstMsg) {
              System.out.printf("==============> Adjusting # of msgs left from %d based on msgIdx=%d%n", msgLeft, getRes.msgIdx);
              // adjust # of msg to receive based on the first message index
              msgLeft -= (getRes.msgIdx - 1);
              System.out.printf("==============> Adjusted # of msgs left to %d%n", msgLeft);
              firstMsg = false;
            }
            if (ackMessage(subscriber, topic, getRes.msgIdx)) {
              msgLeft--;
            }
          } else {
            System.err.printf("================> GetRes=%s, msgLeft: %d%n", getRes, msgLeft);
          }
        } catch (Exception ex) {
          System.err.println(ex);
        }
        System.out.printf("MsgLeft: %d%n", msgLeft);
      }
      clientFinished.countDown();
    };
  }

  @Test
  void singleTopic_publisherOnly() {
    int numClients = 2;
    ExecutorService exec = Executors.newFixedThreadPool(numClients);
    CountDownLatch clientFinished = new CountDownLatch(1);
    CountDownLatch registerdTopic = new CountDownLatch(1);
    int maxQueueSize = 900;
    int numMsgs = 1000;
    String owner = "foo";
    String topic = "iphone";

    runTest(maxQueueSize, numMsgs, exec, clientFinished, () ->
      exec.submit(buildPublisher(owner, topic, numMsgs, clientFinished, registerdTopic))
    );
  }

  @Test
  void singleTopic_publisher1_consumer1() {
    int numClients = 2;
    ExecutorService exec = Executors.newFixedThreadPool(numClients + 1);
    CountDownLatch clientFinished = new CountDownLatch(numClients);
    CountDownLatch registerdTopic = new CountDownLatch(1);
    int maxQueueSize = 500;
    int numMsgs = 800;
    String owner = "foo";
    String topic = "iphone";

    runTest(maxQueueSize, numMsgs, exec, clientFinished, () -> {
      exec.submit(buildPublisher(owner, topic, numMsgs, clientFinished, registerdTopic));
      exec.submit(buildConsumer("taro", topic, numMsgs, clientFinished, registerdTopic));
    });
  }


  @Test
  void singleTopic_publisher1_consumer2() {
    int numClients = 3;
    ExecutorService exec = Executors.newFixedThreadPool(numClients + 1);
    CountDownLatch clientFinished = new CountDownLatch(numClients);
    CountDownLatch registerdTopic = new CountDownLatch(1);
    int maxQueueSize = 900;
    int numMsgs = 500;
    String owner = "foo";
    String topic = "iphone";

    runTest(maxQueueSize, numMsgs, exec, clientFinished, () -> {
      exec.submit(buildPublisher(owner, topic, numMsgs, clientFinished, registerdTopic));
      exec.submit(buildConsumer("jiro", topic, numMsgs, clientFinished, registerdTopic));
      exec.submit(buildConsumer("taro", topic, numMsgs, clientFinished, registerdTopic));
    });
  }

  @Test
  void singleTopic_publisher1_consumer5() {
    int numClients = 6;
    ExecutorService exec = Executors.newFixedThreadPool(numClients + 1);
    CountDownLatch clientFinished = new CountDownLatch(numClients);
    CountDownLatch registerdTopic = new CountDownLatch(1);
    int maxQueueSize = 900;
    int numMsgs = 500;
    String owner = "foo";
    String topic = "iphone";

    runTest(maxQueueSize, numMsgs, exec, clientFinished, () -> {
      exec.submit(buildPublisher(owner, topic, numMsgs, clientFinished, registerdTopic));
      exec.submit(buildConsumer("jiro", topic, numMsgs, clientFinished, registerdTopic));
      exec.submit(buildConsumer("taro", topic, numMsgs, clientFinished, registerdTopic));
      exec.submit(buildConsumer("yuka", topic, numMsgs, clientFinished, registerdTopic));
      exec.submit(buildConsumer("jun", topic, numMsgs, clientFinished, registerdTopic));
      exec.submit(buildConsumer("kyoko", topic, numMsgs, clientFinished, registerdTopic));
    });
  }
}
