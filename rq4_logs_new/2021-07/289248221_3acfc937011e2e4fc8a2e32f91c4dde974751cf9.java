package uk.gov.hmcts.reform.lrdapi.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import java.io.Serializable;
import java.math.BigInteger;
import java.time.LocalDateTime;

@Entity(name = "court_venue")
@AllArgsConstructor
@Getter
@NoArgsConstructor
@Setter
@Builder
@EqualsAndHashCode
public class CourtVenue implements Serializable {

    @Id
    private BigInteger courtVenueId;

    private String epimmsId;

    private String siteName;

    @CreatedDate
    private LocalDateTime createTime;

    @LastModifiedDate
    private LocalDateTime updatedTime;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "region_id")
    private Region region;

    private String courtTypeId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cluster_id")
    private Cluster cluster;

    private String openForPublic;

    private String courtAddress;

    private String postcode;

    private String phoneNumber;

    private LocalDateTime closedDate;

    private String courtLocationCode;

    private String welshSiteName;

    private String welshCourtAddress;

    private String courtStatus;

    private String courtName;

    private LocalDateTime courtOpenDate;

}