package com.foo.request;

public class PublishReq extends OwnerReq {
  public final String topic;
  public final String msg;

  public PublishReq(String owner, String topic, String msg) {
    super(owner);
    this.topic = topic;
    this.msg = msg;
  }
}
