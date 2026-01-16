package help.desk.mobile.api.service.user;

import help.desk.mobile.api.AbstractUnitTest;
import help.desk.mobile.api.config.security.CustomUserDetailsService;
import help.desk.mobile.api.config.security.UserPrincipal;
import help.desk.mobile.api.controller.area.response.AreaDetailsResponse;
import help.desk.mobile.api.controller.user.response.UserDetailsResponse;
import help.desk.mobile.api.domain.entity.UserEntity;
import help.desk.mobile.api.exception.user.InvalidUserException;
import help.desk.mobile.api.mapper.UserMapper;
import help.desk.mobile.api.repository.user.UserRepository;
import help.desk.mobile.api.service.area.FindAreaDetailsByIdService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Optional;

/**
 * @author eduardo.thums
 */
public class DetailLoggedUserServiceTest extends AbstractUnitTest {

	@InjectMocks
	private DetailLoggedUserService detailLoggedUserService;

	@Mock
	private UserRepository userRepository;

	@Mock
	private UserMapper userMapper;

	@Mock
	private CustomUserDetailsService customUserDetailsService;

	@Mock
	private FindAreaDetailsByIdService findAreaDetailsByIdService;

	@Test
	public void detailLoggedUser() {
		// Arrange
		final Long userId = 1L;
		final Long areaId = 2L;

		final UserEntity mockedUser = new UserEntity();
		mockedUser.setAreaId(areaId);

		final AreaDetailsResponse mockedArea = new AreaDetailsResponse();

		final UserDetailsResponse mockedResponse = new UserDetailsResponse();

		BDDMockito
				.given(customUserDetailsService.getUser())
				.willReturn(setupUser(userId));

		BDDMockito
				.given(userRepository.findById(userId))
				.willReturn(Optional.of(mockedUser));

		BDDMockito
				.given(findAreaDetailsByIdService.findDetailsById(areaId))
				.willReturn(mockedArea);

		BDDMockito
				.given(userMapper.toUserDetailsResponse(mockedUser, mockedArea))
				.willReturn(mockedResponse);

		// Act
		final UserDetailsResponse response = detailLoggedUserService.detailLoggedUser();

		// Assert
		Assert.assertEquals(mockedResponse, response);
	}


	@Test(expected = InvalidUserException.class)
	public void detailLoggedUser_throwsInvalidUserException_whenGivenLoggedUserIdDoesNotExist() {
		// Arrange  
		BDDMockito.given(customUserDetailsService.getUser())
				.willReturn(setupUser(1L));

		// Act
		detailLoggedUserService.detailLoggedUser();
	}

	private UserPrincipal setupUser(Long id) {
		return UserPrincipal
				.builder()
				.id(id)
				.build();
	}
}