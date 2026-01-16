package org.folio.rest.handlers;

import static org.folio.repository.UserSummaryRepository.USER_SUMMARY_TABLE_NAME;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.folio.domain.OpenLoan;
import org.folio.domain.UserSummary;
import org.folio.repository.UserSummaryRepository;
import org.folio.rest.TestBase;
import org.folio.rest.jaxrs.model.ItemCheckedOutEvent;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ItemCheckedOutEventHandlerTest extends TestBase {
  private static final ItemCheckedOutEventHandler eventHandler =
    new ItemCheckedOutEventHandler(postgresClient);
  private static final UserSummaryRepository userSummaryRepository =
    new UserSummaryRepository(postgresClient);

  @Before
  public void beforeEach(TestContext context) {
    super.resetMocks();
    deleteAllFromTable(USER_SUMMARY_TABLE_NAME);
  }

  @Test
  public void userSummaryShouldBeCreatedWhenDoesntExist(TestContext context) {
    String userId = randomId();
    String loanId = randomId();
    DateTime dueDate = DateTime.now();

    ItemCheckedOutEvent event = new ItemCheckedOutEvent()
      .withUserId(userId)
      .withLoanId(loanId)
      .withDueDate(dueDate.toDate());

    String summaryId = waitFor(eventHandler.handle(event));

    UserSummary userSummaryToCompare = new UserSummary()
      .withUserId(userId)
      .withNumberOfLostItems(0)
      .withOutstandingFeeFineBalance(BigDecimal.ZERO)
      .withOpenLoans(Collections.singletonList(new OpenLoan()
        .withLoanId(loanId)
        .withDueDate(dueDate.toDate())
        .withRecall(false)));

    checkUserSummary(summaryId, userSummaryToCompare, context);
  }

  @Test
  public void shouldAddOpenLoanWhenUserSummaryExists(TestContext context) {
    String userId = randomId();
    String loanId = randomId();
    DateTime dueDate = DateTime.now();

    List<OpenLoan> existingOpenLoans = new ArrayList<>();
    existingOpenLoans.add(new OpenLoan()
      .withLoanId(randomId())
      .withDueDate(dueDate.toDate())
      .withRecall(false));

    UserSummary existingUserSummary = new UserSummary()
      .withUserId(userId)
      .withNumberOfLostItems(0)
      .withOutstandingFeeFineBalance(BigDecimal.ZERO)
      .withOpenLoans(existingOpenLoans);

    waitFor(userSummaryRepository.save(existingUserSummary));

    ItemCheckedOutEvent event = new ItemCheckedOutEvent()
      .withUserId(userId)
      .withLoanId(loanId)
      .withDueDate(dueDate.toDate());

    String summaryId = waitFor(eventHandler.handle(event));

    existingOpenLoans.add(new OpenLoan()
      .withLoanId(loanId)
      .withDueDate(dueDate.toDate())
      .withRecall(false));

    checkUserSummary(summaryId, existingUserSummary, context);
  }

  @Test
  public void shouldFailWhenOpenLoanWithTheSameLoanIdExists(TestContext context) {
    String userId = randomId();
    String loanId = randomId();
    DateTime dueDate = DateTime.now();

    List<OpenLoan> existingOpenLoans = new ArrayList<>();
    existingOpenLoans.add(new OpenLoan()
      .withLoanId(loanId)
      .withDueDate(dueDate.toDate())
      .withRecall(false));

    UserSummary existingUserSummary = new UserSummary()
      .withUserId(userId)
      .withNumberOfLostItems(0)
      .withOutstandingFeeFineBalance(BigDecimal.ZERO)
      .withOpenLoans(existingOpenLoans);

    waitFor(userSummaryRepository.save(existingUserSummary));

    ItemCheckedOutEvent event = new ItemCheckedOutEvent()
      .withUserId(userId)
      .withLoanId(loanId)
      .withDueDate(dueDate.toDate());

    context.assertNull(waitFor(eventHandler.handle(event)));
  }

  private void checkUserSummary(String summaryId, UserSummary userSummaryToCompare,
    TestContext context) {

    UserSummary userSummary = waitFor(userSummaryRepository.get(summaryId)).orElseThrow(() ->
      new AssertionError("User summary was not found: " + summaryId));

    context.assertEquals(userSummaryToCompare.getUserId(), userSummary.getUserId());
    context.assertEquals(0, userSummaryToCompare.getOutstandingFeeFineBalance().compareTo(
      userSummary.getOutstandingFeeFineBalance()));
    context.assertEquals(0, userSummary.getNumberOfLostItems());
    context.assertEquals(userSummaryToCompare.getOpenLoans().size(),
      userSummary.getOpenLoans().size());

    IntStream.range(0, userSummary.getOpenLoans().size())
      .forEach(i -> {
        OpenLoan openLoan = userSummary.getOpenLoans().get(i);
        OpenLoan openLoanToCompare = userSummaryToCompare.getOpenLoans().get(i);
        context.assertEquals(openLoanToCompare.getLoanId(), openLoan.getLoanId());
        context.assertEquals(openLoanToCompare.getDueDate(), openLoan.getDueDate());
        context.assertEquals(openLoanToCompare.getReturnedDate(), openLoan.getReturnedDate());
        context.assertEquals(openLoanToCompare.getRecall(), openLoan.getRecall());
      });
  }
}