package dev.forkingaround.zootopia.facades;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

@SpringBootTest
public class EncoderFacadeTest {

    @Autowired
    EncoderFacade facade;

    @MockBean
    PasswordEncoder passwordEncoder;

    @BeforeEach
    void setUp() {
        // passwordEncoder = new BCryptPasswordEncoder();
        //facade = new EncoderFacade(passwordEncoder, null);
    }

    // @Disabled
    @Test
    void testEncodeBcrypt() {
        String type = "bcrypt";
        String data = "password";

        PasswordEncoder encoder = new BCryptPasswordEncoder();
        String encoded = encoder.encode(data);

        when(passwordEncoder.encode(data)).thenReturn(encoded);
        String passwordEncoded = facade.encode(type, data);

        assertThat(encoder.matches(data, passwordEncoded), is(true));
    }
}