package um.programacion2;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import um.programacion2.libro.*;
import um.programacion2.prestamo.*;
import um.programacion2.usuario.*;

@TestConfiguration
public class TestConfig {
    @Bean
    public LibroRepository libroRepository() {
        return new LibroRepositoryImpl();
    }

    @Bean
    public LibroService libroService(LibroRepository libroRepository) {
        return new LibroServiceImpl(libroRepository);
    }

	@Bean
	public LibroController libroController(LibroService libroService) {
		return new LibroController(libroService);
	}

	@Bean
	public UsuarioRepository usuarioRepository() {
		return new UsuarioRepositoryImpl() {
		};
	}

	@Bean
	public UsuarioService usuarioService(UsuarioRepository usuarioRepository) {
		return new UsuarioServiceImpl(usuarioRepository);
	}

	@Bean
	public UsuarioController usuarioController(UsuarioService usuarioService) {
		return new UsuarioController(usuarioService);
	}

	@Bean
	public PrestamoRepository prestamoRepository() {
		return new PrestamoRepositoryImpl() {
		};
	}

	@Bean
	public PrestamoService prestamoService(PrestamoRepository prestamoRepository) {
		return new PrestamoServiceImpl(prestamoRepository);
	}

	@Bean
	public PrestamoController prestamoController(PrestamoService prestamoService) {
		return new PrestamoController(prestamoService);
	}
}