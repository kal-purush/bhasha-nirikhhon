package disney.challenge.repositories;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import disney.challenge.entities.Usuario;

@SpringBootTest
class IUsuarioRepositoryTest {
	
	@Autowired
	IUsuarioRepository iUsuarioRepository;

	
	@Test
	void itShouldGetAllDetail() {
		
		//given
		String username = "string";
		//when
		Usuario result = this.iUsuarioRepository.findByUserName(username);
		//then
		Usuario expected = new Usuario("string","$2a$10$rOjVkGoF1jmNYDymitlLg.4YY7RBb7qOou8q/fFU.sLwAgkI7rA3C","pruebasprogramacion8@gmail.com");
		Long id = (long) 3;
		expected.setId(id);
		assertThat(result).isEqualTo(expected);

	}

}