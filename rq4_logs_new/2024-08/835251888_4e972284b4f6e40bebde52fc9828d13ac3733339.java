package dev.forkingaround.zootopia.facades.encryptations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Base64;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Base64EncoderTest {
    private Base64Encoder base64Encoder;

    @BeforeEach
    void setUp() {
        base64Encoder = new Base64Encoder();
    }

    @Test
    void testDecode() {
        String dataToDecode = "password";
        String encodedData = Base64.getEncoder().encodeToString(dataToDecode.getBytes());

        String password = base64Encoder.decode(encodedData);

        assertThat(password, is(dataToDecode));
    }

    @Test
    void testEncode() {
        String dataToDecode = "password";

        String encodedPassword = base64Encoder.encode(dataToDecode);

        byte[] decode = Base64.getDecoder().decode(encodedPassword);
        String password = new String(decode);

        assertThat(password, is(dataToDecode));
    }
}