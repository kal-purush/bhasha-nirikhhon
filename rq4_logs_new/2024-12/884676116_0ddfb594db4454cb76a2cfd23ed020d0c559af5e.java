package com.gathering.gathering_backend.config;

import com.gathering.gathering_backend.entity.Interest;
import com.gathering.gathering_backend.repository.InterestRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

@Configuration
public class InterestInitializer {

    @Bean
    public CommandLineRunner initInterestData(InterestRepository interestRepository) {
        return args -> {
            // 기본 관심사 목록
            List<String> interests = Arrays.asList("디자인", "기획", "프론트엔드", "백엔드", "AI", "DevOps");

            // 관심사 데이터를 중복 없이 추가
            for (String interestName : interests) {
                if (!interestRepository.existsByInterestName(interestName)) {
                    Interest interest = new Interest();
                    interest.setInterestName(interestName);
                    interestRepository.save(interest);
                }
            }

            System.out.println("기본 관심사가 초기화되었습니다.");
        };
    }
}