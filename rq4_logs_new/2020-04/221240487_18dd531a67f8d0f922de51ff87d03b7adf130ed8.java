package uk.gov.justice.hmpps.datacompliance.repository.jpa.model.retention;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import uk.gov.justice.hmpps.datacompliance.repository.jpa.model.referral.ReferralResolution;
import uk.gov.justice.hmpps.datacompliance.repository.jpa.model.retention.manual.ManualRetention;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"retentionReasonId"})
@ToString(exclude = {"referralResolution"}) // to avoid circular reference
@Table(name = "RETENTION_REASON")
public class RetentionReason {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "RETENTION_REASON_ID", nullable = false)
    private Long retentionReasonId;

    @NotNull
    @ManyToOne
    @JoinColumn(name = "RESOLUTION_ID", nullable = false)
    private ReferralResolution referralResolution;

    @ManyToOne
    @JoinColumn(name = "MANUAL_RETENTION_ID")
    private ManualRetention manualRetention;
}