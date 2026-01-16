package org.folio.domain;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiPredicate;

import org.folio.rest.jaxrs.model.PatronBlockLimit;

public enum Condition {
  // IDs come from resources\templates\db_scripts\populate-patron-block-conditions.sql
  MAX_NUMBER_OF_ITEMS_CHARGED_OUT("3d7c52dc-c732-4223-8bf8-e5917801386f",
    (summary, limit) -> summary.getOpenLoans().size() > limit.getValue()
  ),

  MAX_NUMBER_OF_LOST_ITEMS("72b67965-5b73-4840-bc0b-be8f3f6e047e",
    (summary, limit) -> summary.getNumberOfLostItems() > limit.getValue()
  ),

  MAX_NUMBER_OF_OVERDUE_ITEMS("584fbd4f-6a34-4730-a6ca-73a6a6a9d845",
    (summary, limit) -> summary.getOpenLoans().stream()
      .filter(OpenLoan::isOverdue)
      .count() > limit.getValue()
  ),

  MAX_NUMBER_OF_OVERDUE_RECALLS("e5b45031-a202-4abb-917b-e1df9346fe2c",
    (summary, limit) -> summary.getOpenLoans().stream()
      .filter(loan -> loan.isOverdue() && loan.getRecall())
      .count() > limit.getValue()
  ),

  RECALL_OVERDUE_BY_MAX_NUMBER_OF_DAYS("08530ac4-07f2-48e6-9dda-a97bc2bf7053",
    (summary, limit) -> summary.getOpenLoans().stream()
      .filter(OpenLoan::isOverdue)
      .filter(OpenLoan::getRecall)
      .map(OpenLoan::getOverdueDays)
      .anyMatch(days -> days > limit.getValue())
  ),

  MAX_OUTSTANDING_FEE_FINE_BALANCE("cf7a0d5f-a327-4ca1-aa9e-dc55ec006b8a",
    (summary, limit) -> summary.getOutstandingFeeFineBalance()
      .compareTo(BigDecimal.valueOf(limit.getValue())) > 0
  );

  private final String id;
  private final BiPredicate<UserSummary, PatronBlockLimit> limitEvaluator;
  private static final Map<String, Condition> idIndex = new HashMap<>(Condition.values().length);

  static {
    for (Condition condition : Condition.values()) {
      idIndex.put(condition.id, condition);
    }
  }

  Condition(String id, BiPredicate<UserSummary, PatronBlockLimit> limitEvaluator) {
    this.id = id;
    this.limitEvaluator = limitEvaluator;
  }

  public String getId() {
    return id;
  }

  public static Condition getById(String id) {
    return idIndex.get(id);
  }

  public boolean isLimitExceeded(UserSummary summary, PatronBlockLimit limit) {
    return limitEvaluator.test(summary, limit);
  }

  public static boolean isConditionLimitExceeded(UserSummary summary, PatronBlockLimit limit) {
    return idIndex.get(limit.getConditionId())
      .limitEvaluator.test(summary, limit);
  }

}