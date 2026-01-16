package uk.gov.justice.hmpps.datacompliance.repository.jpa.model.retention;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import uk.gov.justice.hmpps.datacompliance.repository.jpa.model.retention.manual.ManualRetention;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@Entity
@DiscriminatorValue(RetentionReasonManual.MANUAL_RETENTION)
public class RetentionReasonManual extends RetentionReason {

    public static final String MANUAL_RETENTION = "MANUAL_RETENTION";

    @OneToOne(mappedBy = "retentionReason", cascade = CascadeType.PERSIST)
    private ManualRetentionLinkTable manualRetentionLinkTable;

    public RetentionReasonManual() {
        super(null, null, MANUAL_RETENTION);
    }

    public RetentionReasonManual setManualRetention(final ManualRetention manualRetention) {
        this.manualRetentionLinkTable = ManualRetentionLinkTable.builder()
                .retentionReason(this)
                .manualRetention(manualRetention)
                .build();
        return this;
    }

    public ManualRetention getManualRetention() {
        return manualRetentionLinkTable.getManualRetention();
    }

    @Data
    @Entity
    @Builder
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(of = {"retentionReasonManualId"})
    @Table(name = "RETENTION_REASON_MANUAL")
    private static class ManualRetentionLinkTable {

        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        @Column(name = "RETENTION_REASON_MANUAL_ID", nullable = false)
        private Long retentionReasonManualId;

        @ManyToOne
        @JoinColumn(name = "RETENTION_REASON_ID")
        private RetentionReasonManual retentionReason;

        @ManyToOne
        @JoinColumn(name = "MANUAL_RETENTION_ID")
        private ManualRetention manualRetention;

    }
}