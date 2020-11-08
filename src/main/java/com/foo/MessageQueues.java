package com.foo;

public interface MessageQueues {
  class GetResult {
    public enum Type {
      Message,
      NoMessage,
      MessageEvicted,
      Error,
    }
    public final GetResult.Type type;
    public final long idx;
    public final String msg;

    private GetResult(GetResult.Type type, long idx, String msg) {
      this.type = type;
      this.idx = idx;
      this.msg = msg;
    }

    public static GetResult error() { return new GetResult(GetResult.Type.Error, 0, null); }
    public static GetResult noMessage() { return new GetResult(GetResult.Type.NoMessage, 0, null); }
    public static GetResult oldMessage() { return new GetResult(Type.MessageEvicted, 0, null); }
    public static GetResult message(long idx, String msg) { return new GetResult(GetResult.Type.Message, idx, msg); }
  }
  void dispose();

  boolean register(String owner, String topic);
  boolean publish(String owner, String topic, String msg);
  boolean subscribe(String subscriber, String topic);
  GetResult get(String subscriber, String topic);
  boolean ack(String subscriber, String topic, long msgIdx);
}
