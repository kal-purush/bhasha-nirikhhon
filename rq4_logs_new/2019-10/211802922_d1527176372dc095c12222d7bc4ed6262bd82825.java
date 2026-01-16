package com.ezgroceries.password;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

public class PasswordUtil {

    public static void main(String[] args) {
        BCryptPasswordEncoder bCryptPasswordEncoder = new BCryptPasswordEncoder();
        System.out.println(String.format("user %s", bCryptPasswordEncoder.encode("user")));
        System.out.println(String.format("admin %s", bCryptPasswordEncoder.encode("admin")));
    }

}