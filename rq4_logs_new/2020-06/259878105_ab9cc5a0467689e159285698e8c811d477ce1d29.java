package org.folio.rest.handlers;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.folio.domain.OpenLoan;
import org.folio.domain.UserSummary;
import org.folio.rest.jaxrs.model.ItemDeclaredLostEvent;
import org.folio.rest.persist.PostgresClient;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class ItemDeclaredLostEventHandler extends EventHandler<ItemDeclaredLostEvent> {

  public ItemDeclaredLostEventHandler(Map<String, String> okapiHeaders, Vertx vertx) {
    super(okapiHeaders, vertx);
  }

  public ItemDeclaredLostEventHandler(PostgresClient postgresClient) {
    super(postgresClient);
  }

  @Override
  public Future<String> handle(ItemDeclaredLostEvent event) {
    return userSummaryRepository.findByUserIdOrBuildNew(event.getUserId())
      .compose(summary -> updateUserSummary(summary, event))
      .onComplete(result -> logResult(result, event));
  }

  private Future<String> updateUserSummary(UserSummary userSummary,
    ItemDeclaredLostEvent event) {

    // increment the number of lost items
    if (userSummary.getNumberOfLostItems() == null) {
      userSummary.setNumberOfLostItems(0);
    }
    userSummary.setNumberOfLostItems(userSummary.getNumberOfLostItems() + 1);

    // remove open loan from a summary
    List<OpenLoan> newOpenLoans = userSummary.getOpenLoans().stream()
      .filter(loan -> !loan.getLoanId().equals(event.getLoanId()))
      .collect(Collectors.toList());
    userSummary.setOpenLoans(newOpenLoans);

    return userSummaryRepository.upsert(userSummary, userSummary.getId());
  }
}