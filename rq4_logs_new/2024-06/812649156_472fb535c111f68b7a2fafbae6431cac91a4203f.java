/* Licensed under Apache-2.0 2024. */
package github.benslabbert.vertxdaggeriam.web.dto;

import com.google.auto.value.AutoBuilder;
import com.google.common.collect.ImmutableList;
import github.benslabbert.jsonwriter.annotation.JsonWriter;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Set;

@JsonWriter
public record InvalidRequestErrorsResponseDto(List<InvalidRequestFieldsResponseDto> errors) {

  public static Builder builder() {
    return new AutoBuilder_InvalidRequestErrorsResponseDto_Builder();
  }

  public static InvalidRequestErrorsResponseDto fromJson(JsonObject json) {
    return InvalidRequestErrorsResponseDto_JsonWriter.fromJson(json);
  }

  public static Set<String> missingRequiredFields(JsonObject json) {
    return InvalidRequestErrorsResponseDto_JsonWriter.missingRequiredFields(json);
  }

  public JsonObject toJson() {
    return InvalidRequestErrorsResponseDto_JsonWriter.toJson(this);
  }

  @AutoBuilder
  public interface Builder {

    Builder errors(List<InvalidRequestFieldsResponseDto> errors);

    ImmutableList.Builder<InvalidRequestFieldsResponseDto> errorsBuilder();

    InvalidRequestErrorsResponseDto build();
  }
}