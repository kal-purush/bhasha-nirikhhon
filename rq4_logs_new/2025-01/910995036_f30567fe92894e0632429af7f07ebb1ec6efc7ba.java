package com.example.lifeonhana.global.util;

import org.springframework.boot.CommandLineRunner;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class PasswordEncoderUtil implements CommandLineRunner {

	@Override
	public void run(String... args) {
		BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();

		// 테스트용 비밀번호 암호화
		String rawPassword = "password123";
		String encodedPassword = encoder.encode(rawPassword);

		System.out.println("\n========== 테스트용 비밀번호 ==========");
		System.out.println("원본 비밀번호: " + rawPassword);
		System.out.println("암호화된 비밀번호: " + encodedPassword);
		System.out.println("====================================\n");
	}
}