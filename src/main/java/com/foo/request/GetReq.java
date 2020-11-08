package com.foo.request;

public class GetReq extends SubscReq {
  public final String topic;

  public GetReq(String user, String topic) {
    super(user);
    this.topic = topic;
  }
}
