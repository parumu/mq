package com.foo;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

public class Cfg {
  public final String host;
  public final int port;
  public final String redisHost;
  public final int redisPort;
  public final int maxMsgSize;
  public final int maxQueueSize;
  public final int maxNameLen;
  public final int maxTopics;
  public final int maxSubscribers;

  // made package private for testing
  Cfg(
    String host,
    int port,
    String redisHost,
    int redisPort,
    int maxMsgSize,
    int maxQueueSize,
    int maxNameLen,
    int maxTopics,
    int maxSubscribers
  ) {
    this.host = host;
    this.port = port;
    this.redisHost = redisHost;
    this.redisPort = redisPort;
    this.maxMsgSize = maxMsgSize;
    this.maxQueueSize = maxQueueSize;
    this.maxNameLen = maxNameLen;
    this.maxTopics = maxTopics;
    this.maxSubscribers = maxSubscribers;
  }

  public static Cfg load() {
    InputStream is = null;
    try {
      Properties props = new Properties();
      String cfgFile = "config.properties";

      is = Cfg.class.getClassLoader().getResourceAsStream(cfgFile);
      if (is == null) {
        throw new FileNotFoundException(String.format("Config file '%s' is missing", cfgFile));
      }
      props.load(is);

      String host = props.getProperty("host");
      int port = Integer.parseInt(props.getProperty("port"));
      String redisHost = props.getProperty("redisHost");
      int redisPort = Integer.parseInt(props.getProperty("redisPort"));
      int maxMsgSize = Integer.parseInt(props.getProperty("maxMsgSize"));
      int maxQueueSize = Integer.parseInt(props.getProperty("maxQueueSize"));
      int maxNameLen = Integer.parseInt(props.getProperty("maxNameLen"));
      int maxTopics = Integer.parseInt(props.getProperty("maxTopics"));
      int maxSubscribers = Integer.parseInt(props.getProperty("maxSubscribers"));

      return new Cfg(host, port, redisHost, redisPort, maxMsgSize, maxQueueSize, maxNameLen, maxTopics, maxSubscribers);
    }
    catch (Exception ex) {
      throw new IllegalStateException("Failed to load config file: ", ex);

    } finally {
      try { if (is != null) is.close(); } catch(Exception ex) { /**/ }
    }
  }
}
