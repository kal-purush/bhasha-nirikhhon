package help.desk.mobile.api.mapper;

import help.desk.mobile.api.AbstractUnitTest;
import help.desk.mobile.api.controller.area.response.AreaDetailsResponse;
import help.desk.mobile.api.controller.user.response.UserDetailsResponse;
import help.desk.mobile.api.domain.entity.UserEntity;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;

/**
 * @author eduardo.thums
 */
public class UserMapperTest extends AbstractUnitTest {

	@InjectMocks
	private UserMapper userMapper;

	@Test
	public void toUserDetailsResponse() {
		// Arrange
		final Long id = 1L;
		final String name = "Roberto Jorge das Neves";
		final String email = "robertojorgedasneves_@bodyfast.com.br";
		final String cpf = "374.701.664-24";
		final String phone = "(83) 3504-8132";

		final UserEntity userEntity = new UserEntity();
		userEntity.setId(id);
		userEntity.setName(name);
		userEntity.setEmail(email);
		userEntity.setCpf(cpf);
		userEntity.setPhone(phone);

		final AreaDetailsResponse area = new AreaDetailsResponse();

		// Act
		final UserDetailsResponse response = userMapper.toUserDetailsResponse(userEntity, area);

		// Assert
		Assert.assertEquals(name, response.getName());
		Assert.assertEquals(email, response.getEmail());
		Assert.assertEquals(cpf, response.getCpf());
		Assert.assertEquals(phone, response.getPhone());
		Assert.assertEquals(area, response.getArea());
	}
}