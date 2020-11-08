package com.foo;

import com.foo.request.*;
import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.foo.MessageQueues.GetResult;

public class Application {
  private final Gson gson = new Gson();
  private final MessageQueues mq;
  private final Logger logger = Logger.getLogger(Application.class.getName());

  // made package private for testing
  Application(MessageQueues mq) {
    this.mq = mq;
  }

  private Publisher<Void> callMqFn(
    HttpServerRequest req,
    HttpServerResponse res,
    Function<String, Mono<Void>> f
  ) {
    return req.receiveContent()
      .switchIfEmpty(Flux.defer(() -> {
        res.status(400);
        return req.receiveContent();
      }))
      .flatMap(x -> Mono.just(x.content().toString(StandardCharsets.UTF_8)))
      .collect(Collectors.joining())
      .flatMap(json -> {
        try {
          return f.apply(json);
        } catch(Exception ex) {
          ex.printStackTrace();
          return res.status(400).send();
        }
      });
  }

  // made package private for testing
  DisposableServer buildHttpServer(String host, int port) {
    return HttpServer.create()
      .host(host)
      .port(port)
      .route(routes ->
        routes
          // publisher routes
            .post("/v1/topic/register", (req, res) ->
              callMqFn(req, res, json -> {
                RegisterReq regReq = gson.fromJson(json, RegisterReq.class);
                logger.info(String.format(
                  "Got register request. { owner: %s, topic: %s }", regReq.owner, regReq.topic));
                if (!mq.register(regReq.owner, regReq.topic)) {
                  return res.status(400).send();
                }
                return res.sendString(Mono.just(regReq.topic)).then();
              })
            )
          .post("/v1/message/publish", (req, res) ->
            callMqFn(req, res, json -> {
              PublishReq pubReq = gson.fromJson(json, PublishReq.class);
              logger.info(String.format(
                "Got publish request. { owner: %s, topic: %s, msg: %s }", pubReq.owner, pubReq.topic, pubReq.msg));
              if (!mq.publish(pubReq.owner, pubReq.topic, pubReq.msg)) {
                return res.status(400).send();
              }
              return res.status(200).send();
            })
          )
          // subscriber routes
          .post("/v1/topic/subscribe", (req, res) ->
            callMqFn(req, res, json -> {
              SubscribeReq subReq = gson.fromJson(json, SubscribeReq.class);
              logger.info(String.format(
                "Got subscribe request. { subscriber: %s, topic: %s }", subReq.subscriber, subReq.topic));
              if (!mq.subscribe(subReq.subscriber, subReq.topic)) {
                return res.status(400).send();
              }
              return res.status(200).send();
            })
          )
          .post("/v1/message/get", (req, res) ->
            callMqFn(req, res, json -> {
              GetReq getReq = gson.fromJson(json, GetReq.class);
              logger.info(String.format(
                "Got get request. { user: %s, topic: %s }", getReq.subscriber, getReq.topic));
              GetResult getRes = mq.get(getReq.subscriber, getReq.topic);

              if (getRes.type == GetResult.Type.Error) {
                return res.status(400).send();
              } else if (getRes.type == GetResult.Type.MessageEvicted) {
                return res.status(230).send();

              } else if (getRes.type == GetResult.Type.NoMessage) {
                return res.status(231).send();

              } else if (getRes.type == GetResult.Type.Message) {
                String getResJson = gson.toJson(new GetRes(getRes.idx, getRes.msg));
                return res.sendString(Mono.just(getResJson)).then();

              } else {
                logger.error(String.format(
                  "Unhandled GetResult.Type found: %s", getRes.type.toString()));
                return res.status(400).send();
              }
            })
          )
          .post("/v1/message/ack", (req, res) ->
            callMqFn(req, res, json -> {
              AckReq ackReq = gson.fromJson(json, AckReq.class);
              logger.info(String.format(
                "Got ack request. { user: %s, topic: %s, msgIdx: %d }", ackReq.subscriber, ackReq.topic, ackReq.msgIdx));
              if (!mq.ack(ackReq.subscriber, ackReq.topic, ackReq.msgIdx)) {
                return res.status(400).send();
              }
              return res.status(200).send();
            })
          )
      ).bindNow();
  }

  public static void main(String[] args) {
    Cfg cfg = Cfg.load();
    MessageQueues mq = new RedisMessageQueues(cfg);

    new Application(mq)
      .buildHttpServer(cfg.host, cfg.port)
      .onDispose()
      .block();
    mq.dispose();
  }
}

