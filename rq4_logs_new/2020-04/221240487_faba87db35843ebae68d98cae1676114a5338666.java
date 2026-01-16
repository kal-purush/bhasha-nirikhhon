package uk.gov.justice.hmpps.datacompliance.repository.jpa.model.referral;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"batchId"})
@Table(name = "OFFENDER_DELETION_BATCH")
public class OffenderDeletionBatch {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "BATCH_ID", nullable = false)
    private Long batchId;

    @NotNull
    @Column(name = "REQUEST_DATE_TIME", nullable = false)
    private LocalDateTime requestDateTime;

    @Column(name = "REFERRAL_COMPLETION_DATE_TIME")
    private LocalDateTime referralCompletionDateTime;

    @NotNull
    @Column(name = "WINDOW_START_DATE_TIME", nullable = false)
    private LocalDateTime windowStartDateTime;

    @NotNull
    @Column(name = "WINDOW_END_DATE_TIME", nullable = false)
    private LocalDateTime windowEndDateTime;
}