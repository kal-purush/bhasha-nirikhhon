package com.github.ssullivan.model.colletions;

import com.github.ssullivan.model.collections.Tuple2;
import com.github.ssullivan.model.collections.Tuple3;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.Test;

public class TupleTests {

  @Test
  public void testNotNullTuple2() {
    try {
      new Tuple2<>(null, null);
    }
    catch (NullPointerException e) {
      MatcherAssert.assertThat(e, Matchers.instanceOf(NullPointerException.class));
    }
  }

  @Test
  public void testNotNullTuple3() {
    try {
      new Tuple3<>(null, null, null);
    }
    catch (NullPointerException e) {
      MatcherAssert.assertThat(e, Matchers.instanceOf(NullPointerException.class));
    }
  }
}