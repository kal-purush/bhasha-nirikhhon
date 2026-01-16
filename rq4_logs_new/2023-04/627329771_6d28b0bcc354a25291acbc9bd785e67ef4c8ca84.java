package Ticket.Repository;

import org.flowershop.domain.tickets.Ticket;
import org.flowershop.domain.tickets.TicketDetail;
import org.flowershop.repository.TicketRepositoryTXT;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TicketRepositoryTXTTest {

    TicketRepositoryTXT ticketRepositoryTXT;

    public TicketRepositoryTXTTest(TicketRepositoryTXT ticketRepositoryTXT) {
        this.ticketRepositoryTXT = ticketRepositoryTXT;
    }

    @DisplayName("Adding new Ticket")
    @Test
    public void addTicket() throws IOException {
        // arrange
        Ticket ticket1 = new Ticket(new Date(), 1L, 345.0, true);
        TicketDetail ticketDetail1 = new TicketDetail(1L, 3, 100.0, 300.0);
        TicketDetail ticketDetail2 = new TicketDetail(2L, 1, 45.0, 45.0);
        ticket1.addTicketDetail(ticketDetail1);
        ticket1.addTicketDetail(ticketDetail2);

        ticketRepositoryTXT.addTicket(ticket1);

        assertEquals(ticket1, ticketRepositoryTXT.getTicketById(ticket1.getId()));
    }

}

