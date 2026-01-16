package org.folio.rest.handlers;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.domain.EventType.ITEM_CHECKED_OUT;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.folio.domain.OpenLoan;
import org.folio.domain.UserSummary;
import org.folio.repository.UserSummaryRepository;
import org.folio.rest.jaxrs.model.ItemCheckedOutEvent;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ItemCheckedOutEventHandler implements Handler<ItemCheckedOutEvent> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String OKAPI_HEADER_TENANT = TENANT.toLowerCase();

  private final UserSummaryRepository userSummaryRepository;

  public ItemCheckedOutEventHandler(Map<String, String> okapiHeaders, Vertx vertx) {
    String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(OKAPI_HEADER_TENANT));
    userSummaryRepository = new UserSummaryRepository(
      PostgresClient.getInstance(vertx, tenantId));
  }

  @Override
  public void handle(ItemCheckedOutEvent event) {
    succeededFuture(event)
      .compose(this::updateUserSummary)
      .onComplete(this::logResult);
  }

  private Future<String> updateUserSummary(ItemCheckedOutEvent event) {
    return userSummaryRepository.findByUserIdOrBuildNew(event.getUserId())
      .compose(summary -> updateUserSummary(summary, event));
  }

  private Future<String> updateUserSummary(UserSummary userSummary,
    ItemCheckedOutEvent event) {

    List<OpenLoan> openLoans = userSummary.getOpenLoans();

    openLoans.stream()
      .filter(loan -> StringUtils.equals(loan.getLoanId(), event.getLoanId()))
      .findFirst()
      .orElseGet(() -> {
        OpenLoan newOpenLoan = new OpenLoan()
          .withLoanId(event.getLoanId())
          .withDueDate(event.getDueDate())
          .withRecall(false)
          .withReturnedDate(null);
        openLoans.add(newOpenLoan);
        return newOpenLoan;
      });

    return userSummaryRepository.upsert(userSummary, userSummary.getId());
  }

  protected void logResult(AsyncResult<String> result) {
    String eventType = ITEM_CHECKED_OUT.name();

    if (result.failed()) {
      log.error("Failed to process event {}", result.cause(), eventType);
    } else {
      log.info("Event {} processed successfully. Affected user summary: {}",
        eventType, result.result());
    }
  }
}