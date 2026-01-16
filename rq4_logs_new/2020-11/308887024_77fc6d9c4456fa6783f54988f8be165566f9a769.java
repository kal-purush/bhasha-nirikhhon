package com.marketplace.domain.classifiedad.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.marketplace.domain.classifiedad.Money;
import java.io.IOException;

public class MoneyDeserializer extends StdDeserializer<Money> {

  public MoneyDeserializer() {
    this(null);
  }

  public MoneyDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public Money deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    return null;
  }
}