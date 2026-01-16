package com.tamecode.springjwt.config;

import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Liqc
 * @date 2020/10/23 11:11
 */
class SecurityConfigTest {

    @Test
    void passwordEncoder() {
        BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
        String encode = passwordEncoder.encode("1234567");
        String encode2 = passwordEncoder.encode("1234567");
        System.out.println(encode);
        System.out.println(encode2);
    }
}