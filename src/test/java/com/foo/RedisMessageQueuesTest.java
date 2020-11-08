package com.foo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.*;

class RedisMessageQueuesTest {
  static final Jedis jedis = new Jedis("localhost");
  final int maxMsgSize = 5;
  final int maxQueueSize = 2;
  final int maxTopics = 1;
  final int maxNameLen = 10;
  final int maxSubscribers = 1;
  final Cfg cfg = new Cfg(
    "localhost", 8080, maxMsgSize, maxQueueSize, maxNameLen, maxTopics, maxSubscribers);

  @BeforeEach
  void flushAll() {
    jedis.flushAll();
  }

  @Test
  void registerTest() {
    RedisMessageQueues mq = new RedisMessageQueues(cfg);
    String owner = "foo";
    String topic = "iphone";

    // too long topic or owner should be rejected
    assertFalse(mq.register(owner, "12345678901"));
    assertFalse(mq.register("12345678901", topic));

    // should be able to register new topic
    assertTrue(mq.register(owner, topic));

    // check if queue is empty and necessary attrs are added
    String queueKey = mq.topicKeyOf(topic);
    assertEquals(0, (long) jedis.llen(queueKey));

    String queryAttrKey = mq.topicAttrKeyOf(topic);
    assertTrue(jedis.hexists(queryAttrKey, RedisMessageQueues.ownerKey));
    assertEquals(owner, jedis.hget(queryAttrKey, RedisMessageQueues.ownerKey));
    assertTrue(jedis.hexists(queryAttrKey, RedisMessageQueues.msgHeadKey));
    assertEquals("0", jedis.hget(queryAttrKey, RedisMessageQueues.msgHeadKey));

    // should not be able to register existing topic
    assertFalse(mq.register(owner, topic));

    // should fail if try to register more than maxTopics number of topics
    assertFalse(mq.register(owner, "new-topic"));
  }

  @Test
  void publishTest() {
    RedisMessageQueues mq = new RedisMessageQueues(cfg);
    String owner = "foo";
    String topic = "iphone";

    // non existing topic should fail
    assertFalse(mq.publish(owner, topic, "papa"));

    // register topic
    assertTrue(mq.register(owner, topic));

    // bad owner should fail
    assertFalse(mq.publish(owner + "-bogus", topic, ""));

    String queueKey = mq.topicKeyOf(topic);
    assertEquals(0, (long) jedis.llen(queueKey));   // should be initially empty

    // too big message should fail
    String bigMsg = "123456";
    assertTrue(bigMsg.getBytes().length > maxMsgSize);
    assertFalse(mq.publish(owner, topic, bigMsg));
    assertEquals(0, (long) jedis.llen(queueKey));

    // message of the same size as msgSize should succeed
    String eqMsg = "12345";
    assertEquals(maxMsgSize, eqMsg.getBytes().length);
    assertTrue(mq.publish(owner, topic, eqMsg));
    assertEquals(eqMsg, jedis.lindex(queueKey, 0) );
    assertEquals(1, (long) jedis.llen(queueKey));

    // add small message and that should make the queue full
    String smallMsg = eqMsg.substring(0, eqMsg.length() - 1);
    assertTrue(mq.publish(owner, topic, smallMsg));
    assertEquals(maxQueueSize, (long) jedis.llen(queueKey));
    assertEquals(smallMsg, jedis.lindex(queueKey, 0));
    assertEquals(eqMsg, jedis.lindex(queueKey, 1) );

    // since queue is full, adding another message should evict oldest message
    String msg = "msg";
    assertTrue(mq.publish(owner, topic, msg));
    assertEquals(maxQueueSize, (long) jedis.llen(queueKey));
    assertEquals(msg, jedis.lindex(queueKey, 0));
    assertEquals(smallMsg, jedis.lindex(queueKey, 1) );
  }

  @Test
  void subscribeTest() {
    RedisMessageQueues mq = new RedisMessageQueues(cfg);
    String owner = "foo";
    String topic = "iphone";
    String subscriber1 = "zap";
    String subscriber2 = "matt";
    String msg = "hello";

    // non existing topic should fail
    assertFalse(mq.publish(owner, topic, "papa"));

    // register topic
    assertTrue(mq.register(owner, topic));

    // too long subscriber should fail
    assertFalse(mq.subscribe("12345678901", topic));

    // subscribing to existing topic should succeed
    assertTrue(mq.subscribe(subscriber1, topic));
    String queueUserKey = mq.topicSubscKeyOf(topic);
    String nextIdx1 = jedis.hget(queueUserKey, subscriber1);
    assertNotNull(nextIdx1);
    assertEquals("1", nextIdx1);  // should point to upcoming next index

    // next index should point to the next upcoming message
    mq.publish(owner, topic, msg);

    assertTrue(mq.subscribe(subscriber2, topic));
    String nextIdx2 = jedis.hget(queueUserKey, subscriber2);
    assertNotNull(nextIdx2);
    assertEquals("2", nextIdx2);  // should point to upcoming next index

    // fail if already subscribed before
    assertFalse(mq.subscribe(subscriber1, topic));

    // should fails if more than maximum number of subscribers tries subscribe
    assertFalse(mq.subscribe("another-subscriber", topic));
  }

  @Test
  void getAndAckTest() {
    RedisMessageQueues mq = new RedisMessageQueues(cfg);
    String owner = "foo";
    String topic = "iphone";
    String subscriber = "zap";
    String msg1 = "hello";
    String msg2 = "world";
    String msg3 = "tokyo";
    String msg4 = "chiba";
    String msg5 = "gifu";

    assertTrue(mq.register(owner, topic));

    // non existing topic should fail
    assertEquals(MessageQueues.GetResult.Type.Error, mq.get(subscriber, "bad-topic").type);

    // if unsubscribed topic should fail
    assertEquals(MessageQueues.GetResult.Type.Error, mq.get(subscriber, topic).type);

    assertTrue(mq.subscribe(subscriber, topic));

    // subscriber should be pointing to next upcoming index and should get NoMessage
    assertEquals(MessageQueues.GetResult.Type.NoMessage, mq.get(subscriber, topic).type);

    assertTrue(mq.publish(owner, topic, msg1));

    // subscriber should get message just published (msg1)
    MessageQueues.GetResult res1 = mq.get(subscriber, topic);
    assertEquals(MessageQueues.GetResult.Type.Message, res1.type);
    assertEquals(1, res1.idx);
    assertEquals(msg1, res1.msg);

    assertTrue(mq.publish(owner, topic, msg2));

    // although new msg (msg2) is published, subscriber should still get msg1 since msg1 is not acked
    // message index should remain 1. new message should have index 2
    MessageQueues.GetResult res2 = mq.get(subscriber, topic);
    assertEquals(MessageQueues.GetResult.Type.Message, res2.type);
    assertEquals(1, res2.idx);
    assertEquals(msg1, res2.msg);

    // ack msg 2 should fail since msg1 is not acked yet
    assertFalse(mq.ack(subscriber, topic, 2));

    // ack msg 1 should succeed
    assertTrue(mq.ack(subscriber, topic, 1));

    // now subscriber should get msg 2
    MessageQueues.GetResult res3 = mq.get(subscriber, topic);
    assertEquals(MessageQueues.GetResult.Type.Message, res3.type);
    assertEquals(2, res3.idx);
    assertEquals(msg2, res3.msg);

    // ack msg 2 should succeed now
    assertTrue(mq.ack(subscriber, topic, 2));

    // public new message (msg 3)
    assertTrue(mq.publish(owner, topic, msg3));

    // publish messages to expel message 3 out of queue
    assertTrue(mq.publish(owner, topic, msg4));
    assertTrue(mq.publish(owner, topic, msg5));

    // no longer able to get msg3 since the message has been evicted
    MessageQueues.GetResult res4 = mq.get(subscriber, topic);
    assertEquals(res4.type, MessageQueues.GetResult.Type.MessageEvicted);

    // but evicted message can be acked
    assertTrue(mq.ack(subscriber, topic, 3));

    // msg4 can be obtained since it is still in the queue
    MessageQueues.GetResult res5 = mq.get(subscriber, topic);
    assertEquals(MessageQueues.GetResult.Type.Message, res5.type);
    assertEquals(4, res5.idx);
    assertEquals(msg4, res5.msg);
  }
}
