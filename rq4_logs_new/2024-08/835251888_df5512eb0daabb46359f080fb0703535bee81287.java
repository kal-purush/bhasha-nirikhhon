package dev.forkingaround.zootopia.facades.encryptations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.lang.reflect.Field;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

public class BcryptEncoderTest {

    BcryptEncoder encoder;
    BCryptPasswordEncoder passwordEncoder;

    @BeforeEach
    void setUp() {
        passwordEncoder = new BCryptPasswordEncoder();
        encoder = new BcryptEncoder(passwordEncoder);
    }

    @Test
    void testBcryptEncoderHavePropertyBcryptPasswordEncoder() {
        Field[] declaredFields = encoder.getClass().getDeclaredFields();

        assertThat(encoder.encoder, instanceOf(BCryptPasswordEncoder.class));
        assertThat(declaredFields.length, is(1));
    }

    @Test
    void testEncodingDataWithBcrypt() {
        String dataToEncode = "password";
        String encodedData = encoder.encode(dataToEncode);

        assertThat(passwordEncoder.matches(dataToEncode, encodedData), is(true));
    }
}