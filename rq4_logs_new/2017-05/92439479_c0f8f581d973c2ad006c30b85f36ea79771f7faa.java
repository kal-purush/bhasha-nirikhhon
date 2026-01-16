package fiuba.aninfo.ticket;

import fiuba.aninfo.entity.User;
import fiuba.aninfo.entity.Card;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import cucumber.api.java.en.Then;

import org.junit.Assert;

public class IssueTicketMarkingTest {
  private Card card = null;

  @Given("^a card created as internal")
  public void givenACardCreatedAsInternal() throws Exception {
    card = new Card();
    card.classification = Card.INTERNAL;
    Assert.assertEquals(card.getClassification(), Classification.INTERNAL);
  }

  @When("^a user wants to tag it as external")
  public void whenAUserWantsToTagItAsExternal(Classification classification) throws Exception {
    card.setClassification(classification);
    Assert.assertEquals(classification, Classification.EXTERNAL);
  }

  @Then("^it should be left marked as external")
  public void thenItShouldBetaggedAsExternal() throws Exception {
    Assert.assertEquals(card.getClassification(), Classification.EXTERNAL);
  }
}