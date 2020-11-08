package com.foo.request;

public class AckReq extends SubscReq {
  public final String topic;
  public final int msgIdx;

  public AckReq(String user, String topic, int msgIdx) {
    super(user);
    this.topic = topic;
    this.msgIdx = msgIdx;
  }
}
