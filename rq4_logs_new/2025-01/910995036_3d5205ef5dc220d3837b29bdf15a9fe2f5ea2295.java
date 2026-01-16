package com.example.lifeonhana;

import static com.example.lifeonhana.entity.Account.Bank.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.Year;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.MockMvc;

import com.example.lifeonhana.entity.Account;
import com.example.lifeonhana.entity.Mydata;
import com.example.lifeonhana.entity.User;
import com.example.lifeonhana.repository.AccountRepository;
import com.example.lifeonhana.repository.MydataRepository;
import com.example.lifeonhana.repository.UserRepository;
import com.example.lifeonhana.service.JwtService;
//
// @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// @AutoConfigureMockMvc
// class AccountControllerIntegrationTest {
//
// 	@Autowired
// 	private MockMvc mockMvc;
//
// 	@Autowired
// 	private AccountRepository accountRepository;
//
// 	@Autowired
// 	private MydataRepository myDataRepository;
//
// 	@Autowired
// 	private UserRepository userRepository; // User 엔티티 관련 Repository 필요
//
// 	private Mydata savedMyData;
//
// 	@BeforeEach
// 	void setUp() {
// 		// User 생성 및 저장
// 		User user = new User();
// 		user.setUserId(1L); // user_id는 직접 설정
// 		user.setUsername("test_user");
// 		user.setPassword("password"); // 필요한 필드 값 설정
// 		userRepository.save(user);
//
// 		// Mydata 생성 및 저장
// 		Mydata myData = new Mydata();
// 		myData.setUser(user);
// 		myData.setTotalAsset(new BigDecimal("350000.00"));
// 		myData.setDepositAmount(new BigDecimal("10000.00"));
// 		myData.setSavingsAmount(new BigDecimal("20000.00"));
// 		myData.setLoanAmount(new BigDecimal("5000.00"));
// 		myData.setStockAmount(new BigDecimal("15000.00"));
// 		myData.setRealEstateAmount(new BigDecimal("300000.00"));
// 		myData.setLastUpdatedAt(LocalDateTime.now());
// 		myData.setPensionStartYear(Year.of(2035));
// 		savedMyData = myDataRepository.save(myData);
//
// 		// Account 생성 및 저장
// 		Account mainAccount = new Account();
// 		mainAccount.setBank(HANA);
// 		mainAccount.setAccountNumber("123-456");
// 		mainAccount.setAccountName("Main Account");
// 		mainAccount.setBalance(new BigDecimal("1000.00"));
// 		mainAccount.setServiceAccount(Account.ServiceAccount.SALARY);
// 		mainAccount.setMydata(savedMyData); // 저장된 Mydata와 연관 설정
// 		accountRepository.save(mainAccount);
//
// 		Account otherAccount = new Account();
// 		otherAccount.setBank(KB);
// 		otherAccount.setAccountNumber("789-012");
// 		otherAccount.setAccountName("Other Account");
// 		otherAccount.setBalance(new BigDecimal("2000.00"));
// 		otherAccount.setServiceAccount(Account.ServiceAccount.OTHER);
// 		otherAccount.setMydata(savedMyData); // 동일한 Mydata와 연관 설정
// 		accountRepository.save(otherAccount);
// 	}
//
// 	@AfterEach
// 	void tearDown() {
// 		// 데이터 정리
// 		accountRepository.deleteAll();
// 		myDataRepository.deleteAll();
// 		userRepository.deleteAll();
// 	}
//
// 	@Test
// 	void getAccounts_ReturnsAccountListSuccessfully() throws Exception {
// 		// When & Then
// 		mockMvc.perform(get("/api/account")
// 				.header("Authorization", "Bearer validToken"))
// 			.andExpect(status().isOk())
// 			.andExpect(jsonPath("$.message").value("계좌 목록 조회 성공"))
// 			.andExpect(jsonPath("$.data.mainAccount.accountNumber").value("123-456"));
// 	}
//
// 	@Test
// 	void getAccounts_InvalidToken_ReturnsUnauthorized() throws Exception {
// 		mockMvc.perform(get("/api/account")
// 				.header("Authorization", "InvalidToken"))
// 			.andExpect(status().isUnauthorized());
// 	}
// }