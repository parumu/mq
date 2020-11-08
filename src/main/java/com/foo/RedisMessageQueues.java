package com.foo;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;

import static java.lang.Long.parseLong;

public class RedisMessageQueues implements MessageQueues {
  private final int maxMsgSize;
  private final int maxQueueSize;
  private final int maxNameLen;
  private final String maxTopics;  // string since Redis stores # of topics as string
  private final int maxSubscribers;
  private final Logger logger = Logger.getLogger(RedisMessageQueues.class.getName());
  private final JedisPool jedisPool;

  // made package private for testing
  static final String ownerKey = "owner";
  static final String msgHeadKey = "msg-head";
  static final String global = "global";
  static final String numTopicsKey = "num-topics";
  static final String numSubscsKey = "num-subscs";

  public RedisMessageQueues(Cfg cfg) {
    this.jedisPool = new JedisPool(new JedisPoolConfig(), cfg.host);
    this.maxMsgSize = cfg.maxMsgSize;
    this.maxQueueSize = cfg.maxQueueSize;
    this.maxNameLen = cfg.maxNameLen;
    this.maxTopics = String.valueOf(cfg.maxTopics);
    this.maxSubscribers = cfg.maxSubscribers;

    Jedis jedis = new Jedis(cfg.jedisHost, cfg.jedisPort);
    if (!jedis.hexists(global, numTopicsKey)) {
      jedis.hset(global, numTopicsKey, "0");
    }
  }

  // made package private for testing
  String topicKeyOf(String topic) { return topic.concat("-topic"); }
  String topicAttrKeyOf(String topic) { return topic.concat("-topic-attr"); }
  String topicSubscKeyOf(String topic) { return topic.concat("-topic-subsc"); }

  @Override
  public void dispose() {
    jedisPool.close();
  }

  @Override
  public boolean register(String owner, String topic) {
    // error if owner or topic is too long
    if (topic.length() > maxNameLen || owner.length() > maxNameLen) {
      return false;
    }
    String topicKey = topicKeyOf(topic);
    String topicAttrKey = topicAttrKeyOf(topic);

    try(Jedis jedis = jedisPool.getResource()) {
      jedis.watch(topicKey);
       String numTopicsStr = jedis.hget(global, numTopicsKey);

      // error if reaching the maximum # of topics
      if (numTopicsStr.equals(maxTopics)) {
        logger.warn("Maximum number of topics has been registered");
        return false;
      };

      // error if topic is already registered
      if (jedis.hexists(topicAttrKey, ownerKey)) {
        logger.warn(String.format("Topic '%s' has already been registered", topic));
        return false;
      }
      Transaction tx = jedis.multi();
      try {
        // add a new queue and set the owner
        tx.hset(topicAttrKey, ownerKey, owner);
        tx.hset(topicAttrKey, msgHeadKey, "0");   // msg id starts from 1 and 0 means no msg
        tx.hset(topicAttrKey, numSubscsKey, "0");
        long numTopics = Long.parseLong(numTopicsStr);
        tx.hset(global, numTopicsKey, String.valueOf(numTopics + 1));

        if (tx.exec() == null) {
          logger.warn("Topickey has been modified during transaction");
          return false;
        }
      } catch(Exception ex) { // assuming critical cases such as oom error
        logger.error(String.format("Critical error occurred while executing transaction: %s", ex.getMessage()));
        tx.discard();
        return false;
      }
      logger.info(String.format("Registered { owner: %s, topic: %s }", owner, topic));
      return true;
    }
  }

  @Override
  public boolean publish(String owner, String topic, String msg) {
    String topicKey = topicKeyOf(topic);
    String topicAttrKey = topicAttrKeyOf(topic);

    try(Jedis jedis = jedisPool.getResource()) {
      // check if topic exists
      if (!jedis.hexists(topicAttrKey, ownerKey)) {
        logger.warn(String.format("Topic '%s' not exist", topic));
        return false;
      }
      // check if registered owner is the same as caller
      String topicOwner = jedis.hget(topicAttrKey, ownerKey);
      if (topicOwner == null || !topicOwner.equals(owner)) {
        logger.warn(String.format("Topic owner expected to be '%s', but got '%s'", owner, topicOwner));
        return false;
      }
      // check if message size is within the limit
      if (msg.getBytes().length > maxMsgSize) {
        logger.warn(String.format("Message max size is %d. but got %d", maxMsgSize, msg.getBytes().length));
        return false;
      }

      while(true) {
        // add msg to the topic
        jedis.watch(topicKey);
        long msgHeadIdx = Long.parseLong(jedis.hget(topicAttrKey, msgHeadKey));
        Long queueSize = jedis.llen(topicKey);

        Transaction tx = jedis.multi();
        try {
          // adjust queue size if needed
          if (queueSize == maxQueueSize) {
            // remove oldest message
            tx.rpop(topicKey);   // discard oldest message
            logger.info(String.format("Evicted oldest message from '%s'", topicKey));
          }
          tx.lpush(topicKey, msg);
          tx.hset(topicAttrKey, msgHeadKey, String.valueOf(msgHeadIdx + 1));

          if (tx.exec() == null) {
            logger.warn("TopicKey has been modified during transaction");
            continue;
          }
        } catch (Exception ex) {
          logger.error(String.format("Critical error occurred while executing transaction: %s", ex.getMessage()));
          tx.discard();
          return false;
        }
        logger.info(String.format("Published { owner: %s, topic: %s, msg: %s }", owner, topic, msg));
        return true;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return false;
    }
  }

  @Override
  public boolean subscribe(String subscriber, String topic) {
    // error if subscriber is too long
    if (subscriber.length() > maxNameLen) {
      return false;
    }
    String topicAttrKey = topicAttrKeyOf(topic);

    try(Jedis jedis = jedisPool.getResource()) {
      // check if topic exists
      if (!jedis.hexists(topicAttrKey, ownerKey)) {
        logger.warn(String.format("Topic '%s' not exist", topic));
        return false;
      }
      // check if max # of subscribers has been reached
      long numSubscs = Long.parseLong(jedis.hget(topicAttrKey, numSubscsKey));
      if (numSubscs == maxSubscribers) {
        logger.warn(String.format("Maximum number of subscribers has been reached for %s", topicAttrKey));
        return false;
      }

      // check if already subscribed
      String topicSubscKey = topicSubscKeyOf(topic);
      if (jedis.hexists(topicSubscKey, subscriber)) {
        logger.warn(String.format("Already subscribed to topic '%s'", topic));
        return false;
      }
      // point to the index next to the current head
      String msgHeadIdxStr = jedis.hget(topicAttrKeyOf(topic), msgHeadKey);
      long msgHeadIdx = Long.parseLong(msgHeadIdxStr);
      jedis.hset(topicSubscKey, subscriber, String.valueOf(msgHeadIdx + 1));

      logger.info(String.format("'%s' subscribed to '%s'", subscriber, topic));
      return true;

    } catch (Exception ex) {
      ex.printStackTrace();
      return false;
    }
  }

  @Override
  public GetResult get(String subscriber, String topic) {
    String topicKey = topicKeyOf(topic);
    String topicAttrKey = topicAttrKeyOf(topic);

    try(Jedis jedis = jedisPool.getResource()) {
      // check if topic exists
      if (!jedis.hexists(topicAttrKey, ownerKey)) {
        logger.warn(String.format("Topic '%s' not exist", topic));
        return GetResult.error();
      }
      // check if subscriber subscribed to the topic
      String topicSubscKey = topicSubscKeyOf(topic);
      String nextMsgIdxStr = jedis.hget(topicSubscKey, subscriber);
      if (nextMsgIdxStr == null) {
        logger.warn(String.format("'%s' hasn't subscribed to '%s'", subscriber, topic));
        return GetResult.error();
      }
      long nextMsgIdx = Long.parseLong(nextMsgIdxStr);
      long queueLen = jedis.llen(topicKey);
      long msgHeadIdx = Long.parseLong(jedis.hget(topicAttrKeyOf(topic), msgHeadKey));

      // return if message is not yet published
      if (nextMsgIdx > msgHeadIdx) {
        logger.info(String.format("Topic '%s' has no new message", topic));
        return GetResult.noMessage();
      }
      // calc list index of the message to get
      long listIdx = msgHeadIdx - nextMsgIdx;

      // can get if message is in queue
      if (listIdx < queueLen) {
        String msg = jedis.lindex(topicKey, listIdx);
        logger.info(String.format("Returned message idx=%d from topic '%s'", nextMsgIdx, topic));
        return GetResult.message(nextMsgIdx, msg);

      } else { // message has been dropped from queue
        logger.info(String.format("Message idx=%d has been dropped from topic '%s'", nextMsgIdx, topic));
        return GetResult.oldMessage();
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return GetResult.error();
    }
  }

  @Override
  public boolean ack(String subscriber, String topic, long msgIdx) {
    String topicKey = topicKeyOf(topic);
    String topicAttrKey = topicAttrKeyOf(topic);

    try(Jedis jedis = jedisPool.getResource()) {
      // check if topic exists
      if (!jedis.hexists(topicAttrKey, ownerKey)) {
        logger.warn(String.format("Topic '%s' not exist", topic));
        return false;
      }
      // check if subscriber subscribed to the topic
      String topicSubscKey = topicSubscKeyOf(topic);
      String nextMsgIdxStr = jedis.hget(topicSubscKey, subscriber);
      if (nextMsgIdxStr == null) {
        logger.warn(String.format("'%s' hasn't subscribed to '%s'", subscriber, topic));
        return false;
      }
      while(true) {
        jedis.watch(topicKey, topicSubscKey);
        long nextMsgIdx = parseLong(nextMsgIdxStr);
        long msgHeadIdx = Long.parseLong(jedis.hget(topicAttrKeyOf(topic), msgHeadKey));

        Transaction tx = jedis.multi();
        try {
          // cannot ack if message is not yet published
          if (nextMsgIdx > msgHeadIdx) {
            logger.warn(String.format("Message idx=%d has not been published yet", msgIdx));
            tx.discard();
            return false;
          }
          // can only ack the current unacked message
          if (msgIdx != nextMsgIdx) {
            logger.warn(String.format(
              "Cannot ack message idx=%d since message idx=%d is to be acked next", msgIdx, nextMsgIdx));
            tx.discard();
            return false;
          }
          // ack the current unacked message by updating the nextMsgIdx
          tx.hset(topicSubscKey, subscriber, String.valueOf(nextMsgIdx + 1));
          if (tx.exec() == null) {
            logger.warn("Topickey or TopicSubscKey has been modified during transaction");
            continue;
          }
        } catch (Exception ex) {
          logger.error(String.format("Critical error occurred while executing transaction: %s", ex.getMessage()));
          tx.discard();
          return false;
        }
        logger.info(String.format("Acked message idx=%d in topic '%s'", msgIdx, topic));
        return true;
      }
    } catch(Exception ex) {
      ex.printStackTrace();
      return false;
    }
  }
}
