//package com.bithumb.interest.application;
//
//import com.bithumb.interest.api.dto.InterestResponse;
//import com.bithumb.interest.domain.Interest;
//import com.bithumb.interest.repository.InterestRepository;
//import static org.hamcrest.MatcherAssert.*;
//import static org.hamcrest.Matchers.*;
//import static org.junit.jupiter.api.Assertions.*;
//import static org.mockito.ArgumentMatchers.*;
//import static org.mockito.BDDMockito.*;
//import static org.mockito.Mockito.*;
//
//import java.util.List;
//import java.util.Optional;
//
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
//import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
//import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
//import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
//import org.springframework.test.context.web.WebAppConfiguration;
//
//@WebAppConfiguration
//@ExtendWith(MockitoExtension.class)
//@DataJpaTest
//@AutoConfigureTestDatabase
//@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class})
//class InterestServiceImplTest {
//
//    @InjectMocks
//    InterestServiceImpl interestService;
//
//    @Mock
//    InterestRepository interestRepository;
//
//
//    //dto
//    final Interest interest = Interest.builder()
//            .userId(1)
//            .korean("비트코인")
//            .symbol("BTC")
//            .market("BTC_KRW")
//            .build();
//
//    @BeforeEach
//    void setUp() {
//    }
//
//    @Test
//    @DisplayName("관심코인 조회")
//    void getInterests() {
//        //given
//        given(interestRepository.save(any())).willReturn(InterestResponse.class);
//        Interest resultOutput = interestRepository.save(interest);
//        System.out.println(resultOutput);
//        assertThat(resultOutput, is(resultOutput));
//    }
//
//    @Test
//    void createInterest() {
//        String korean = "비트코인";
//        Interest interest1 = new Interest().builder().korean("비트코인").userId(1).build();
//        final Interest resultOutput= interestRepository.save(interest1);
//        System.out.println(resultOutput.getKorean());
//        assertEquals(korean, resultOutput.getKorean());
//    }
//
//    @Test
//    void deleteInterest() {
//
//    }
//}