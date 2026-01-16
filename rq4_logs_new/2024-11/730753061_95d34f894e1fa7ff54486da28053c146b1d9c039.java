package org.folio.entitlement.domain.model;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RetryInformation {

  @Builder.Default
  private int retriesCount = 0;
  @Builder.Default
  private List<String> errors = new ArrayList<>();

  public RetryInformation incrementRetriesCount() {
    retriesCount++;
    return this;
  }

  public RetryInformation addError(String errorInformation) {
    this.errors.add(errorInformation);
    return this;
  }
}