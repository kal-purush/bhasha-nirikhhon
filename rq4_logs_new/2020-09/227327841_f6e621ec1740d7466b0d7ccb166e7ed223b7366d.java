package eu.europa.ec.fisheries.uvms.incident.arquillian;

import eu.europa.ec.fisheries.uvms.incident.TransactionalTests;
import eu.europa.ec.fisheries.uvms.incident.helper.TicketHelper;
import eu.europa.ec.fisheries.uvms.incident.model.dto.IncidentDto;
import eu.europa.ec.fisheries.uvms.incident.model.dto.enums.IncidentType;
import eu.europa.ec.fisheries.uvms.incident.model.dto.enums.StatusEnum;
import eu.europa.ec.fisheries.uvms.incident.service.ServiceConstants;
import eu.europa.ec.fisheries.uvms.incident.service.domain.entities.Incident;
import eu.europa.ec.fisheries.uvms.incident.service.helper.IncidentHelper;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(Arquillian.class)
public class IncidentHelperTest extends TransactionalTests {

    @Inject
    IncidentHelper incidentHelper;

    @Test
    @OperateOnDeployment("incident")
    public void settingManualModeIntoParkedStatus() {
        IncidentDto newIncident = TicketHelper.createBasicIncidentDto();

        newIncident.setType(IncidentType.MANUAL_POSITION_MODE);
        newIncident.setStatus(StatusEnum.PARKED);
        try {
            incidentHelper.checkIncidentIntegrity(newIncident, null);
            fail();
        }catch (Exception e){
            assertTrue(e.getMessage().contains("does not support being placed in status"));
        }
    }

    @Test
    @OperateOnDeployment("incident")
    public void givingAssetNotSendingAnExpiryDate() {
        IncidentDto newIncident = TicketHelper.createBasicIncidentDto();

        newIncident.setType(IncidentType.ASSET_NOT_SENDING);
        newIncident.setStatus(StatusEnum.INCIDENT_CREATED);
        newIncident.setExpiryDate(Instant.now());

        try {
            incidentHelper.checkIncidentIntegrity(newIncident, null);
            fail();
        }catch (Exception e){
            assertTrue(e.getMessage().contains("does not support having a expiry date"));
        }
    }

    @Test
    @OperateOnDeployment("incident")
    public void setManualModeAsResolved() {
        IncidentDto newIncident = TicketHelper.createBasicIncidentDto();

        newIncident.setType(IncidentType.MANUAL_POSITION_MODE);
        newIncident.setStatus(StatusEnum.RESOLVED);
        newIncident.setExpiryDate(Instant.now());

       IncidentDto checkedIncident = incidentHelper.checkIncidentIntegrity(newIncident, null);
       assertEquals(StatusEnum.RESOLVED, checkedIncident.getStatus());
    }

    @Test
    @OperateOnDeployment("incident")
    public void checkManualIncidentWoOldIncident() {
        IncidentDto newIncident = TicketHelper.createBasicIncidentDto();

        newIncident.setType(IncidentType.MANUAL_POSITION_MODE);
        newIncident.setStatus(StatusEnum.MANUAL_POSITION_MODE);
        Instant now = Instant.now();
        newIncident.setExpiryDate(now);

        IncidentDto checkedIncident = incidentHelper.checkIncidentIntegrity(newIncident, null);

        assertNotEquals(now, checkedIncident.getExpiryDate());
        assertTrue(now.plus(65, ChronoUnit.MINUTES).isBefore(checkedIncident.getExpiryDate()));
        assertTrue(now.plus(66, ChronoUnit.MINUTES).isAfter(checkedIncident.getExpiryDate()));
    }

    @Test
    @OperateOnDeployment("incident")
    public void checkManualIncidentWithRecentMovement() {
        IncidentDto newIncident = TicketHelper.createBasicIncidentDto();

        newIncident.setType(IncidentType.MANUAL_POSITION_MODE);
        newIncident.setStatus(StatusEnum.RECEIVING_VMS_POSITIONS);
        Instant now = Instant.now();
        newIncident.setExpiryDate(now);

        Incident oldIncident = new Incident();
        oldIncident.setMovementId(UUID.randomUUID());
        Instant movementTimestamp = Instant.now().minus(ServiceConstants.MAX_DELAY_BETWEEN_MANUAL_POSITIONS_IN_MINUTES - 30, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MILLIS);
        System.setProperty("MOVEMENT_MOCK_TIMESTAMP", "" + movementTimestamp.toEpochMilli());

        IncidentDto checkedIncident = incidentHelper.checkIncidentIntegrity(newIncident, oldIncident);

        assertEquals(StatusEnum.MANUAL_POSITION_MODE, checkedIncident.getStatus());
        assertNotEquals(now, checkedIncident.getExpiryDate());
        assertEquals(movementTimestamp.plus(65, ChronoUnit.MINUTES), checkedIncident.getExpiryDate());

        System.clearProperty("MOVEMENT_MOCK_TIMESTAMP");
    }

    @Test
    @OperateOnDeployment("incident")
    public void checkManualIncidentWithoutRecentMovement() {
        IncidentDto newIncident = TicketHelper.createBasicIncidentDto();

        newIncident.setType(IncidentType.MANUAL_POSITION_MODE);
        newIncident.setStatus(StatusEnum.RECEIVING_VMS_POSITIONS);
        Instant now = Instant.now();
        newIncident.setExpiryDate(now);

        Incident oldIncident = new Incident();
        oldIncident.setMovementId(UUID.randomUUID());
        Instant movementTimestamp = Instant.now().minus(ServiceConstants.MAX_DELAY_BETWEEN_MANUAL_POSITIONS_IN_MINUTES + 30, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MILLIS);
        System.setProperty("MOVEMENT_MOCK_TIMESTAMP", "" + movementTimestamp.toEpochMilli());

        IncidentDto checkedIncident = incidentHelper.checkIncidentIntegrity(newIncident, oldIncident);

        assertEquals(StatusEnum.MANUAL_POSITION_LATE, checkedIncident.getStatus());
        assertNotEquals(now, checkedIncident.getExpiryDate());
        assertEquals(movementTimestamp.plus(65, ChronoUnit.MINUTES), checkedIncident.getExpiryDate());

        System.clearProperty("MOVEMENT_MOCK_TIMESTAMP");
    }
}