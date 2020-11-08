package com.foo.request;

public class RegisterReq extends OwnerReq {
  public final String topic;

  public RegisterReq(String owner, String topic) {
    super(owner);
    this.topic = topic;
  }
}
