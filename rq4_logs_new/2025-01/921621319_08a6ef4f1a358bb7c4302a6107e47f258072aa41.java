package com.metamong.mt.global.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import com.metamong.mt.member.model.Member;
import com.metamong.mt.member.repository.IMemberRepository;

@Component
public class DataLoader implements CommandLineRunner {

    @Autowired
    private IMemberRepository memberMapper;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Override
    public void run(String... args) throws Exception {
        // 이미 admin 계정이 존재하지 않으면 기본값을 삽입
        if (!memberMapper.existsAdmin()) {
            String encodedPassword = passwordEncoder.encode("1234");  // 비밀번호 암호화

            // 빌더 패턴을 사용하여 admin 계정 생성
            Member admin = Member.builder()
                    .userid("admin")
                    .name("Admin")
                    .password(encodedPassword)
                    .phone("000-0000-0000")
                    .email("admin@example.com")
                    .role("ROLE_ADMIN")
                    .postal_code("00000")
                    .address("서울시 예시구 예시동")
                    .detail_address("상세 주소 예시")
                    .businessName(null)
                    .businessRegistrationNumber(null)
                    .birth(null)
                    .build();

            memberMapper.insertMember(admin);
            System.out.println("Admin user has been created.");
        }
    }
}