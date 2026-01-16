package uk.gov.justice.hmpps.datacompliance.client.elite2api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;

import static uk.gov.justice.hmpps.datacompliance.utils.DateTimeUtils.ISO_LOCAL_DATE_TIME_FORMAT;

@Getter
@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class PendingDeletionsRequest {

    @JsonProperty("dueForDeletionWindowStart")
    @JsonFormat(pattern = ISO_LOCAL_DATE_TIME_FORMAT)
    private LocalDateTime dueForDeletionWindowStart;

    @JsonProperty("dueForDeletionWindowEnd")
    @JsonFormat(pattern = ISO_LOCAL_DATE_TIME_FORMAT)
    private LocalDateTime dueForDeletionWindowEnd;

    @JsonProperty("requestId")
    private String requestId;
}