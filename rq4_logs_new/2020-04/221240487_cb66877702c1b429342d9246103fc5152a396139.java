package uk.gov.justice.hmpps.datacompliance.repository.jpa.model.referral;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"resolutionId"})
@Table(name = "REFERRAL_RESOLUTION")
public class ReferralResolution {

    public enum ResolutionType {
        RETAINED,
        DELETION_GRANTED,
        DELETED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "RESOLUTION_ID", nullable = false)
    private Long resolutionId;

    @NotNull
    @OneToOne
    @JoinColumn(name = "REFERRAL_ID", nullable = false)
    private OffenderDeletionReferral offenderDeletionReferral;

    @NotNull
    @Enumerated(EnumType.STRING)
    @Column(name = "RESOLUTION_TYPE", nullable = false)
    private ReferralResolution.ResolutionType resolutionType;

    @Column(name = "RESOLUTION_DATE_TIME")
    private LocalDateTime resolutionDateTime;

    public boolean isType(final ResolutionType type) {
        return type == resolutionType;
    }
}