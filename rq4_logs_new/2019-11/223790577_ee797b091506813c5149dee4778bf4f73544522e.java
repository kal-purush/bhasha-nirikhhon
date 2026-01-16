package selenide_with_assert;

import com.codeborne.selenide.ElementsCollection;

import static com.codeborne.selenide.CollectionCondition.sizeGreaterThan;
import static com.codeborne.selenide.Condition.text;
import static com.codeborne.selenide.Selenide.$$;

public class SearchResultsPage {
  private ElementsCollection results = $$("#res .g");

  public void checkResultsSizeIsAtLeast(int expectedSize) {
    results.shouldHave(sizeGreaterThan(expectedSize));
  }

  public void checkResultHasTest(int index, String expectedText) {
    results.get(index).shouldHave(text(expectedText));
  }

  public ElementsCollection getResults() {
    return results;
  }
}