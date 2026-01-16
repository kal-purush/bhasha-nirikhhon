package com.honestefforts.fixengine.model.engine_service_poc;

import com.honestefforts.fixengine.model.message.FixMessage;
import com.honestefforts.fixengine.model.validation.ValidationError;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FixMessageResponseV1 {

  @NonNull
  List<FixMessage> response;

  List<ValidationError> errors;
}