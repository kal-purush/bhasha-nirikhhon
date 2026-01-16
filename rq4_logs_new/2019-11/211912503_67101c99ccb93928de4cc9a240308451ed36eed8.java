package help.desk.mobile.api.service.user;

import help.desk.mobile.api.AbstractUnitTest;
import help.desk.mobile.api.config.security.CustomUserDetailsService;
import help.desk.mobile.api.config.security.UserPrincipal;
import help.desk.mobile.api.domain.entity.TicketEntity;
import help.desk.mobile.api.domain.entity.UserEntity;
import help.desk.mobile.api.exception.user.InvalidUserException;
import help.desk.mobile.api.repository.user.UserRepository;
import help.desk.mobile.api.service.ticket.FindAllTicketsByAuthorIdService;
import help.desk.mobile.api.service.ticket.SaveAllTicketsService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.*;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DeleteLoggedUserServiceTest extends AbstractUnitTest {

    @InjectMocks
    private DeleteLoggedUserService deleteLoggedUserService;

    @Mock
    private UserRepository userRepository;

    @Mock
    private CustomUserDetailsService customUserDetailsService;

    @Mock
    private FindAllTicketsByAuthorIdService findAllTicketsByAuthorIdService;

    @Mock
    private SaveAllTicketsService saveAllTicketsService;

    @Captor
    private ArgumentCaptor<List<TicketEntity>> ticketsArgumentCaptor;

    @Captor
    private ArgumentCaptor<UserEntity> userArgumentCaptor;

    @Test
    public void deleteLoggedUser() {
        // Arrange
        final Long loggedUserId = 1L;

        final UserPrincipal loggedUser = setupUser(loggedUserId);
        final TicketEntity mockedTicket = new TicketEntity();
        final UserEntity mockedUser = new UserEntity();

        BDDMockito
                .given(customUserDetailsService.getUser())
                .willReturn(loggedUser);

        BDDMockito
                .given(findAllTicketsByAuthorIdService.findAllByAuthorId(loggedUserId))
                .willReturn(Collections.singletonList(mockedTicket));

        BDDMockito
                .given(userRepository.findById(loggedUserId))
                .willReturn(Optional.of(mockedUser));

        // Act
        deleteLoggedUserService.deleteLoggedUser();

        BDDMockito
                .then(saveAllTicketsService)
                .should()
                .saveAll(ticketsArgumentCaptor.capture());

        BDDMockito
                .then(userRepository)
                .should()
                .save(userArgumentCaptor.capture());

        // Assert
        final List<TicketEntity> persistedTickets = ticketsArgumentCaptor.getValue();

        Assert.assertFalse(persistedTickets.isEmpty());

        final TicketEntity persistedTicket = persistedTickets.get(0);
        Assert.assertTrue(persistedTicket.isDeleted());

        final UserEntity persistedUser = userArgumentCaptor.getValue();
        Assert.assertTrue(persistedUser.isDeleted());
    }

    @Test(expected = InvalidUserException.class)
    public void deleteLoggedUser_throwsInvalidUserException() {
        // Arrange
        final Long loggedUserId = 1L;

        final UserPrincipal loggedUser = setupUser(loggedUserId);
        final TicketEntity mockedTicket = new TicketEntity();

        BDDMockito
                .given(customUserDetailsService.getUser())
                .willReturn(loggedUser);

        BDDMockito
                .given(findAllTicketsByAuthorIdService.findAllByAuthorId(loggedUserId))
                .willReturn(Collections.singletonList(mockedTicket));

        // Act
        deleteLoggedUserService.deleteLoggedUser();
    }

    private UserPrincipal setupUser(Long userId){
        return UserPrincipal
                .builder()
                .id(userId)
                .build();
    }
}