package help.desk.mobile.api.service.ticket;

import help.desk.mobile.api.AbstractUnitTest;
import help.desk.mobile.api.domain.entity.TicketEntity;
import help.desk.mobile.api.repository.ticket.TicketRepository;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.*;

import java.util.Collections;
import java.util.List;

public class SaveAllTicketsServiceTest extends AbstractUnitTest {

    @InjectMocks
    private SaveAllTicketsService saveAllTicketsService;

    @Mock
    private TicketRepository ticketRepository;

    @Captor
    private ArgumentCaptor<List<TicketEntity>> ticketsArgumentCaptor;

    @Test
    public void saveAll() {
        // Arrange
        final TicketEntity mockedTicket = new TicketEntity();

        // Act
        saveAllTicketsService.saveAll(Collections.singletonList(mockedTicket));

        BDDMockito
                .then(ticketRepository)
                .should()
                .saveAll(ticketsArgumentCaptor.capture());

        // Assert
        final List<TicketEntity> persistedTickets = ticketsArgumentCaptor.getValue();

        Assert.assertFalse(persistedTickets.isEmpty());

        final TicketEntity persistedTicket = persistedTickets.get(0);
        Assert.assertEquals(mockedTicket,persistedTicket);
    }
}