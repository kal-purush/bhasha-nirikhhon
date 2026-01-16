package com.seproject.account.common.authentication.provider;

import com.seproject.account.account.domain.FormAccount;
import com.seproject.account.common.domain.LoginHistory;
import com.seproject.account.common.domain.LoginPreventUser;
import com.seproject.account.common.domain.repository.LoginHistoryRepository;
import com.seproject.account.common.domain.repository.LoginPreventUserRepository;
import com.seproject.error.errorCode.ErrorCode;
import com.seproject.error.exception.CustomAuthenticationException;
import com.seproject.error.exception.PasswordIncorrectException;
import com.seproject.global.AccountSetup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Transactional
@SpringBootTest
class UsernameAuthenticationProviderTest {

    @Autowired AccountSetup accountSetup;
    @Autowired LoginHistoryRepository loginHistoryRepository;
    @Autowired
    LoginPreventUserRepository loginPreventUserRepository;
    @Autowired EntityManager em;

    @Autowired UsernameAuthenticationProvider usernameAuthenticationProvider;


    @Test
    public void 로그인_성공() throws Exception {
        FormAccount formAccount = accountSetup.createFormAccount();

        UsernamePasswordAuthenticationToken authentication
                = new UsernamePasswordAuthenticationToken(formAccount, "1234", formAccount.getAuthorities());

        Authentication validAuthentication = usernameAuthenticationProvider.authenticate(authentication);

        assertEquals(validAuthentication.getName(),authentication.getName());
        assertEquals(validAuthentication.getCredentials(),authentication.getCredentials());
    }
    @Test
    public void 로그인_시도_실패() throws Exception {
        FormAccount formAccount = accountSetup.createFormAccount();

        UsernamePasswordAuthenticationToken authentication
                = new UsernamePasswordAuthenticationToken(formAccount, "12345", formAccount.getAuthorities());

        PasswordIncorrectException passwordIncorrectException = assertThrows(PasswordIncorrectException.class, () -> {
            usernameAuthenticationProvider.authenticate(authentication);
        });

        em.flush(); em.clear();

        boolean isPrevented = loginPreventUserRepository.isPreventUser(formAccount.getLoginId(), LocalDateTime.now());

        Assertions.assertFalse(isPrevented);
    }

    @Test
    public void 로그인_시도_5번이상() throws Exception {
        FormAccount formAccount = accountSetup.createFormAccount();

        for (int i = 0; i < 4; i++) {
            LoginHistory loginHistory = new LoginHistory(formAccount.getLoginId());
            loginHistoryRepository.save(loginHistory);
        }

        UsernamePasswordAuthenticationToken authentication
                = new UsernamePasswordAuthenticationToken(formAccount, "12345", formAccount.getAuthorities());

        PasswordIncorrectException passwordIncorrectException = assertThrows(PasswordIncorrectException.class, () -> {
            usernameAuthenticationProvider.authenticate(authentication);
        });

        em.flush(); em.clear();

        boolean isPrevented = loginPreventUserRepository.isPreventUser(formAccount.getLoginId(), LocalDateTime.now());

        Assertions.assertTrue(isPrevented);
    }

    @Test
    public void 로그인_10분_정지_아이디() throws Exception {
        FormAccount formAccount = accountSetup.createFormAccount();
        LoginPreventUser loginPreventUser = new LoginPreventUser(formAccount.getLoginId());
        loginPreventUserRepository.save(loginPreventUser);
        em.flush(); em.clear();

        UsernamePasswordAuthenticationToken authentication
                = new UsernamePasswordAuthenticationToken(formAccount, "12345", formAccount.getAuthorities());

        CustomAuthenticationException exception = assertThrows(CustomAuthenticationException.class, () -> {
            usernameAuthenticationProvider.authenticate(authentication);
        });

        Assertions.assertEquals(exception.getErrorCode(), ErrorCode.LOGIN_PREVENT_USER);
        Optional<LoginPreventUser> byId = loginPreventUserRepository.findById(loginPreventUser.getId());
        Assertions.assertTrue(byId.isPresent());
        LoginPreventUser findLoginPreventUser = byId.get();

        Assertions.assertEquals(findLoginPreventUser.getId(),loginPreventUser.getId());
        Assertions.assertEquals(findLoginPreventUser.getLoginId(),loginPreventUser.getLoginId());
        Assertions.assertEquals(findLoginPreventUser.getLocalDateTime(),loginPreventUser.getLocalDateTime());
    }
}