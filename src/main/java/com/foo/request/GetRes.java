package com.foo.request;

public class GetRes {
  public final long msgIdx;
  public final String msg;

  public GetRes(long msgIdx, String msg) {
    this.msgIdx = msgIdx;
    this.msg = msg;
  }

  @Override
  public String toString() {
    return String.format("GetRes(msg: %s, msgIdx: %d)", msg, msgIdx);
  }
}
