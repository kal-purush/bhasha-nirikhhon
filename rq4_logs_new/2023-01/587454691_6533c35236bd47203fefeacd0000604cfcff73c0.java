package br.com.study.api.resources;

import br.com.study.api.domain.User;
import br.com.study.api.domain.dto.UserDTO;
import br.com.study.api.services.UserService;
import br.com.study.api.services.impl.UserServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.modelmapper.ModelMapper;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class UserResourceTest {
    public static final Integer ID       = 1;
    public static final String  NAME     = "Felipe";
    public static final String  EMAIL    = "fsdiasdf@gmail.com";
    public static final String  PASSWORD = "123";
    public static final int INDEX = 0;
    public static final String OBJETO_NAO_ENCONTRADO = "Objeto não encontrado.";
    public static final String O_E_MAIL_JA_FOI_CADASTRADO_NA_NOSSA_BASE_DE_DADOS = "O e-mail já foi cadastrado na nossa base de dados.";

    @InjectMocks
    private UserResource resource;

    @Mock
    private ModelMapper mapper;

    @Mock
    private UserServiceImpl service;

    private User user;
    private UserDTO userDTO;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        startUser();
    }

    @Test
    void findById() {
    }

    @Test
    void findAll() {
    }

    @Test
    void create() {
    }

    @Test
    void update() {
    }

    @Test
    void delete() {
    }

    private void startUser() {
        user = new User(ID, NAME, EMAIL, PASSWORD);
        userDTO = new UserDTO(ID, NAME, EMAIL, PASSWORD);
    }
}