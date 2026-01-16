package com.github.ssullivan.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ssullivan.utils.ShortUuid;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.Test;

public class FacilityJacksonTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testSerialization() throws IOException {
    Facility facility = new Facility();
    facility.setFeedId(ShortUuid.randomShortUuid());
    facility.setState("VA");
    facility.setServiceCodes(ImmutableSet.of("TEST"));
    facility.setCategoryCodes(ImmutableSet.of("TEST"));

    final String json = OBJECT_MAPPER.writeValueAsString(facility);
    final Facility fromJson = OBJECT_MAPPER.readValue(json, Facility.class);

    MatcherAssert.assertThat(fromJson.getState(), Matchers.equalTo("VA"));
    MatcherAssert.assertThat(fromJson.hasAnyOf("TEST"), Matchers.equalTo(true));

  }
}