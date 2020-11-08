package com.foo.request;

public class SubscribeReq extends SubscReq {
  public final String topic;

  public SubscribeReq(String user, String topic) {
    super(user);
    this.topic = topic;
  }
}
