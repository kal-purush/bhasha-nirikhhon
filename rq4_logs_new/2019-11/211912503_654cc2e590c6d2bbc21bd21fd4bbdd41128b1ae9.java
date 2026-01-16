package help.desk.mobile.api.service.ticket;

import help.desk.mobile.api.AbstractUnitTest;
import help.desk.mobile.api.domain.entity.TicketEntity;
import help.desk.mobile.api.repository.ticket.TicketRepository;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FindAllTicketsByAuthorIdServiceTest extends AbstractUnitTest {

    @InjectMocks
    private FindAllTicketsByAuthorIdService findAllTicketsByAuthorIdService;

    @Mock
    private TicketRepository ticketRepository;

    @Test
    public void findAllByAuthorId() {
        // Arrange
        final Long authorId = 1L;

        final TicketEntity mockedTicket = new TicketEntity();

        BDDMockito
                .given(ticketRepository.findAllByAuthorId(authorId))
                .willReturn(Collections.singletonList(mockedTicket));

        // Act
        final List<TicketEntity> responseList = findAllTicketsByAuthorIdService.findAllByAuthorId(authorId);

        // Assert
        Assert.assertFalse(responseList.isEmpty());

        final TicketEntity response = responseList.get(0);
        Assert.assertEquals(mockedTicket, response);
    }
}