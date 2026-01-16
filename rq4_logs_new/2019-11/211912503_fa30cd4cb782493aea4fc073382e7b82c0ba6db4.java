package help.desk.mobile.api.service.ticketstatus;

import help.desk.mobile.api.AbstractUnitTest;
import help.desk.mobile.api.domain.entity.TicketStatusEntity;
import help.desk.mobile.api.repository.ticketstatus.TicketStatusRepository;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.*;

/**
 * @author eduardo.thums
 */
public class SaveTicketStatusServiceTest extends AbstractUnitTest {

	@InjectMocks
	private SaveTicketStatusService saveTicketStatusService;

	@Mock
	private TicketStatusRepository ticketStatusRepository;

	@Captor
	private ArgumentCaptor<TicketStatusEntity> ticketStatusEntityArgumentCaptor;

	@Test
	public void saveTicketStatus() {
		// Arrange
		final Long ticketStatusId = 1L;

		final TicketStatusEntity ticketStatusEntity = new TicketStatusEntity();

		final TicketStatusEntity mockedPersistedTicketStatus = new TicketStatusEntity();
		mockedPersistedTicketStatus.setId(ticketStatusId);

		BDDMockito.given(ticketStatusRepository.save(ticketStatusEntity))
				.willReturn(mockedPersistedTicketStatus);

		// Act
		final Long response = saveTicketStatusService.saveTicketStatus(ticketStatusEntity);

		// Assert
		BDDMockito.then(ticketStatusRepository)
				.should()
				.save(ticketStatusEntityArgumentCaptor.capture());

		final TicketStatusEntity persistedTicketStatusEntity = ticketStatusEntityArgumentCaptor.getValue();
		Assert.assertEquals(ticketStatusEntity, persistedTicketStatusEntity);

		Assert.assertEquals(ticketStatusId, response);
	}
}