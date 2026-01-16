package uk.gov.hmcts.reform.lrdapi.controllers.response;

import org.junit.Test;
import uk.gov.hmcts.reform.lrdapi.domain.Cluster;
import uk.gov.hmcts.reform.lrdapi.domain.CourtVenue;
import uk.gov.hmcts.reform.lrdapi.domain.Region;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CourtVenueResponseTest {

    @Test
    public void testCourtVenueTransformationToResponse_ClusterAndDatesAbsent() {

        Region region = new Region();
        region.setCreatedTime(LocalDateTime.now());
        region.setDescription("Region XYZ");
        region.setRegionId("123");
        region.setUpdatedTime(LocalDateTime.now());
        region.setWelshDescription("Region ABC");

        CourtVenue courtVenue = CourtVenue.builder()
            .courtTypeId("17")
            .siteName("Aberdeen Tribunal Hearing Centre 1")
            .openForPublic(Boolean.TRUE)
            .epimmsId("123456789")
            .region(region)
            .courtName("ABERDEEN TRIBUNAL HEARING CENTRE 1")
            .courtStatus("Open")
            .postcode("AB11 6LT")
            .courtAddress("AB1, 48 HUNTLY STREET, ABERDEEN")
            .courtVenueId(1L)
            .build();

        CourtVenueResponse courtVenueResponse = new CourtVenueResponse(courtVenue);

        assertEquals("YES", courtVenueResponse.getOpenForPublic());
        assertNull(courtVenueResponse.getClusterId());
        assertNull(courtVenueResponse.getClusterName());
        assertEquals(region.getRegionId(), courtVenueResponse.getRegionId());
        assertEquals(region.getDescription(), courtVenueResponse.getRegion());
        assertNull(courtVenueResponse.getClosedDate());
        assertNull(courtVenueResponse.getCourtOpenDate());
    }

    @Test
    public void testCourtVenueTransformationToResponse_ClusterAndDatesPresent() {

        Cluster cluster = new Cluster();
        cluster.setClusterId("456");
        cluster.setClusterName("ClusterXYZ");
        cluster.setWelshClusterName("ClusterABC");
        cluster.setCreatedTime(LocalDateTime.now());
        cluster.setUpdatedTime(LocalDateTime.now());

        LocalDateTime now = LocalDateTime.now();

        CourtVenue courtVenue = CourtVenue.builder()
            .courtTypeId("17")
            .siteName("Aberdeen Tribunal Hearing Centre 1")
            .openForPublic(Boolean.FALSE)
            .epimmsId("123456789")
            .courtName("ABERDEEN TRIBUNAL HEARING CENTRE 1")
            .courtStatus("Open")
            .cluster(cluster)
            .postcode("AB11 6LT")
            .courtAddress("AB1, 48 HUNTLY STREET, ABERDEEN")
            .courtVenueId(1L)
            .courtOpenDate(now)
            .closedDate(now)
            .build();

        CourtVenueResponse courtVenueResponse = new CourtVenueResponse(courtVenue);

        assertEquals("NO", courtVenueResponse.getOpenForPublic());
        assertEquals(cluster.getClusterId(), courtVenueResponse.getClusterId());
        assertEquals(cluster.getClusterName(), courtVenueResponse.getClusterName());
        assertNull(courtVenueResponse.getRegion());
        assertNull(courtVenueResponse.getRegionId());
        assertEquals(now.toString(), courtVenueResponse.getClosedDate().toString());
        assertEquals(now.toString(), courtVenueResponse.getCourtOpenDate().toString());
    }

}