package com.google.codeu.render;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class ImageUrlMessageTransformerTest {
  private MessageTransformer messageTransformer;

  @Before
  public void setup() {
    messageTransformer = new ImageUrlMessageTransformer();
  }

  private void runTransformTextTest(String input, String expected) {
    String actual = messageTransformer.transformText(input);
    assertEquals(expected, actual);
  }

  @Test
  public void testTransformText() {
    runTransformTextTest("This is the Google logo http://www.google.com/images/logo.png!",
      "This is the Google logo <figure><img src=\"http://www.google.com/images/logo.png\" /> <figcaption></figcaption></figure>!");
    runTransformTextTest("[This is the Google logo] http://www.google.com/images/logo.png!",
        "<figure><img src=\"http://www.google.com/images/logo.png\" /> <figcaption>This is the Google logo</figcaption></figure>!");
    runTransformTextTest("", "");
    runTransformTextTest("This is plain text.", "This is plain text.");
    runTransformTextTest("Check out this cool art. [Link.] https://pbs.twimg.com/media/D1ux_clUkAABe_P.png",
        "Check out this cool art. <figure><img src=\"https://pbs.twimg.com/media/D1ux_clUkAABe_P.png\" /> <figcaption>Link.</figcaption></figure>");
    runTransformTextTest("[Fake caption 1.] [Peanut butter.] https://interfaithsanctuary.org/wp-content/uploads/2017/12/peanut-butter.jpg",
        "[Fake caption 1.] <figure><img src=\"https://interfaithsanctuary.org/wp-content/uploads/2017/12/peanut-butter.jpg\" /> <figcaption>Peanut butter.</figcaption></figure>");
    runTransformTextTest("[Fake caption.] [Peanut butter.] https://interfaithsanctuary.org/wp-content/uploads/2017/12/peanut-butter.jpg "
        + "https://pbs.twimg.com/media/D1ux_clUkAABe_P.png", "[Fake caption.] <figure><img src=\"https://interfaithsanctuary.org/wp-content/uploads/2017/12/peanut-butter.jpg\" /> <figcaption>Peanut butter.</figcaption></figure>"
        + " <figure><img src=\"https://pbs.twimg.com/media/D1ux_clUkAABe_P.png\" /> <figcaption></figcaption></figure>");
  }
}