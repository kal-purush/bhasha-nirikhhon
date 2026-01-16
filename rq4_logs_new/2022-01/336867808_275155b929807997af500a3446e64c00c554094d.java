package io.github.dschanoeh.healthbuddy.notifications;

import io.github.dschanoeh.healthbuddy.Incident;
import io.github.dschanoeh.healthbuddy.configuration.DashboardConfiguration;
import io.github.dschanoeh.healthbuddy.configuration.HealthBuddyConfiguration;
import io.github.dschanoeh.healthbuddy.dto.IncidentHistoryDTO;
import io.github.dschanoeh.healthbuddy.dto.IncidentHistoryEntryDTO;
import io.specto.hoverfly.junit5.HoverflyExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(HoverflyExtension.class)
@ExtendWith(SpringExtension.class)
class IncidentHistoryCollectorTest {
    private static final long TEST_DASHBOARD_WINDOW_DURATION = 60L;
    private static final UUID SERVICE_ID = UUID.randomUUID();
    private static final UUID UNKNOWN_SERVICE_ID = UUID.randomUUID();
    private static final ZonedDateTime serviceStartTime = ZonedDateTime.parse("2022-01-01T00:00:00Z");
    @Autowired
    IncidentHistoryCollector collector;

    @TestConfiguration
    static class TestConfig {

        @Bean
        public HealthBuddyConfiguration HealthBuddyConfiguration() {
            HealthBuddyConfiguration c = new HealthBuddyConfiguration();
            DashboardConfiguration d = new DashboardConfiguration();
            d.setHistoryWindowDuration(TEST_DASHBOARD_WINDOW_DURATION);
            c.setDashboard(d);
            return c;
        }

        @Bean
        public IncidentHistoryCollector IncidentHistoryCollector() {
            return new IncidentHistoryCollector();
        }
    }

    @Test
    void noDataForUnknownService() {
        assertNull(collector.getHistoryForService(UNKNOWN_SERVICE_ID));
    }

    @Test
    void validHistoryForSingleIncident() {
        IncidentHistoryCollector collectorSpy = Mockito.spy(collector);
        Mockito.doReturn(serviceStartTime).when(collectorSpy).getServiceStartTime();

        // Incident is opened 1 minute after service start time
        Mockito.doReturn(serviceStartTime.plusMinutes(1)).when(collectorSpy).getCurrentTime();
        Incident i = Mockito.mock(Incident.class);
        Mockito.doReturn(serviceStartTime.plusMinutes(1)).when(i).getStartDate();
        Mockito.doReturn(Incident.Type.NOT_REACHABLE).when(i).getType();
        Mockito.doReturn(SERVICE_ID).when(i).getServiceId();
        Mockito.doReturn(true).when(i).isOpen();
        collectorSpy.openIncident(i);

        // Incident is closed 5 minutes after service start time
        Mockito.doReturn(serviceStartTime.plusMinutes(5)).when(collectorSpy).getCurrentTime();
        Mockito.doReturn(serviceStartTime.plusMinutes(5)).when(i).getEndDate();
        Mockito.doReturn(false).when(i).isOpen();
        collectorSpy.closeIncident(i);

        Mockito.doReturn(serviceStartTime.plusMinutes(59)).when(collectorSpy).getCurrentTime();
        IncidentHistoryDTO history = collectorSpy.getIncidentHistory(SERVICE_ID);

        assertEquals(TEST_DASHBOARD_WINDOW_DURATION, history.getHistoryMaximum());
        List<IncidentHistoryEntryDTO> historyEntries = history.getHistory();
        assertEquals(4, historyEntries.size());

        // 1 minute of UNKNOWN time in the window where the service didn't run yet
        assertEquals(0, historyEntries.get(0).getStart());
        assertEquals(1, historyEntries.get(0).getEnd());
        assertEquals(IncidentHistoryEntryDTO.Status.UNKNOWN, historyEntries.get(0).getStatus());

        // 1 minute of UP time before the incident
        assertEquals(1, historyEntries.get(1).getStart());
        assertEquals(2, historyEntries.get(1).getEnd());
        assertEquals(IncidentHistoryEntryDTO.Status.UP, historyEntries.get(1).getStatus());

        // 4 minutes of DOWN time before the incident
        assertEquals(2, historyEntries.get(2).getStart());
        assertEquals(6, historyEntries.get(2).getEnd());
        assertEquals(IncidentHistoryEntryDTO.Status.DOWN, historyEntries.get(2).getStatus());

        // Remainder of the window is up
        assertEquals(6, historyEntries.get(3).getStart());
        assertEquals(TEST_DASHBOARD_WINDOW_DURATION, historyEntries.get(3).getEnd());
        assertEquals(IncidentHistoryEntryDTO.Status.UP, historyEntries.get(3).getStatus());
    }
}