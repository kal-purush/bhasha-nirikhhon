package de.ait_tr.g_40_shop.controller;

import de.ait_tr.g_40_shop.domain.dto.ProductDto;
import de.ait_tr.g_40_shop.domain.entity.Role;
import de.ait_tr.g_40_shop.domain.entity.User;
import de.ait_tr.g_40_shop.repository.RoleRepository;
import de.ait_tr.g_40_shop.repository.UserRepository;
import de.ait_tr.g_40_shop.security.sec_dto.TokenResponseDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT) // надо тест
// запускать на рандомном порту, тк порт 8080 занят приложением
@TestMethodOrder(MethodOrderer.OrderAnnotation.class) // чтобы был порядок тестов определенный

class ProductControllerTest {
    @LocalServerPort // служит для сохранения в поле класса значения случайно выбираемого порта,
    // на котором стартует тестовый контейнер сервлетов
    private int port; // его значение запишется в переменную port
    @Autowired
    private UserRepository userRepository;
    @Autowired
    private RoleRepository roleRepository;

    private TestRestTemplate template; // чтобы отправлять http запросы
    private HttpHeaders headers; // тело запроса сюда идет?

    private ProductDto testProduct; // для тестирования нужен сам продукт

    private final String TEST_PRODUCT_TITLE = "Test product title";
    private final BigDecimal TEST_PRODUCT_PRICE = new BigDecimal(777);
    private final String TEST_ADMIN_NAME = "Test Admin";
    private final String TEST_USER_NAME = "Test User";
    private final String TEST_PASSWORD = "Test Password";
    private final String ADMIN_ROLE_TITLE = "ROLE_ADMIN";
    private final String USER_ROLE_TITLE = "ROLE_USER";
    private final String URL_PREFIX = "http://localhost:";
    private final String AUTH_RESOURCE_NAME = "/auth";
    private final String PRODUCTS_RESOURCE_NAME = "/products";
    private final String LOGIN_ENDPOINT = "/login";
    private final String ALL_ENDPOINT = "/all";


    private  final String BEARER_PREFIX = "Bearer ";
    private String adminAccessToken;
    private String userAccessToken;


    @BeforeEach
    public void setUp() {
       template = new TestRestTemplate(); // инициализируем перемнные
       headers = new HttpHeaders();

       testProduct = new ProductDto(); // инициализируем продукт
       testProduct.setTitle( TEST_PRODUCT_TITLE); //не рекомендуется литералы писать в коде,
        // поэтому выносим текст в константу TEST_PRODUCT_TITLE
        testProduct.setPrice(TEST_PRODUCT_PRICE);

        BCryptPasswordEncoder encoder = null; //для шифрования паролей
        Role roleAdmin;
        Role roleUser = null;

        User admin = userRepository.findByUsername(TEST_ADMIN_NAME).orElse(null);
        User user = userRepository.findByUsername(TEST_USER_NAME).orElse(null);

        if (admin == null) {
            encoder = new BCryptPasswordEncoder();
            roleAdmin = roleRepository.findByTitle(ADMIN_ROLE_TITLE).orElse(null);
            roleUser = roleRepository.findByTitle(USER_ROLE_TITLE).orElse(null);

            if (roleAdmin== null || roleUser == null) {
                throw  new RuntimeException("Role admin or role user is missing in the database");

            }

            admin = new User();
            admin.setUsername(TEST_ADMIN_NAME);
            admin.setPassword(encoder.encode(TEST_PASSWORD));
            admin.setRoles(Set.of(roleAdmin, roleUser));

            userRepository.save(admin);
        }

        if (user == null) {
            encoder = encoder == null ? new BCryptPasswordEncoder() : encoder;
            roleUser = roleUser == null ?
                    roleRepository.findByTitle(USER_ROLE_TITLE).orElse(null) : roleUser;

            if (roleUser == null) {
                throw  new RuntimeException("Role user is missing in the database");

            }

            user = new User();
            user.setUsername(TEST_USER_NAME);
            user.setPassword(encoder.encode(TEST_PASSWORD));
            user.setRoles(Set.of(roleUser));

            userRepository.save(user);
        }
        admin.setPassword(TEST_PASSWORD);
        admin.setRoles(null);

        user.setPassword(TEST_PASSWORD);
        user.setRoles(null);

        String url = URL_PREFIX + port + AUTH_RESOURCE_NAME + LOGIN_ENDPOINT;
        HttpEntity<User> request = new HttpEntity<>(admin, headers);

        ResponseEntity<TokenResponseDto> response = template
                .exchange(url, HttpMethod.POST, request, TokenResponseDto.class);

        assertNotNull(response.getBody(), "Auth response body is empty");
        adminAccessToken = BEARER_PREFIX + response.getBody().getAccessToken();

        request = new HttpEntity<>(user, headers);

        response = template
                .exchange(url, HttpMethod.POST, request, TokenResponseDto.class);

        assertNotNull(response.getBody(), "Auth response body is empty");
        userAccessToken = BEARER_PREFIX + response.getBody().getAccessToken();

    }

    @Test
    public void positiveGettingAllProductsWithoutAuthorization() {

        String url = URL_PREFIX + port + PRODUCTS_RESOURCE_NAME + ALL_ENDPOINT;
        HttpEntity<Void> request = new HttpEntity<>(headers);

        ResponseEntity<ProductDto[]> response = template
                .exchange(url, HttpMethod.GET, request, ProductDto[].class);

        assertEquals(HttpStatus.OK, response.getStatusCode(), "Response has unexpected status");
        assertTrue(response.hasBody(), "Response has no body");

    }
    @Test
    public void negativeSavingProductWithoutAuthorization() {
        String url = URL_PREFIX + port + PRODUCTS_RESOURCE_NAME;
        HttpEntity<ProductDto> request = new HttpEntity<>(testProduct, headers);

        ResponseEntity<ProductDto> response = template
                .exchange(url, HttpMethod.POST, request, ProductDto.class);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode(), "Response has unexpected status");
        assertFalse(response.hasBody(), "Response has unexpected body");

    }

}