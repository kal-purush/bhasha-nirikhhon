package com.example;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdifyServiceTest {

  class SenderSpy implements Subscription.Sender {
    String event;

    @Override
    public void send(String event, String body) {
      this.event = event;
    }
  }

  @Test
  @Tag("slow")
  void foo() {
    SenderSpy spy = new SenderSpy();
    AdifyService a = new AdifyService(new Adify(new HerokuGetRequest("adify")), "SESSION_ID,USER_ID,PRODUCT_ID", spy);
    a.execute();
    assertEquals("fixme", spy.event);
  }

}