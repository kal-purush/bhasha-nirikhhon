package ru.wasabi.my_atm.service.impl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import ru.wasabi.my_atm.entity.account.Account;
import ru.wasabi.my_atm.repository.AccountRepository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AccountServiceImplTest {

    @Mock
    AccountRepository accountRepository;

    @InjectMocks
    AccountServiceImpl accountService;

    @Test
    @DisplayName("GET api/v1/accounts")
    void getAllAccounts_ReturnValidResponseEntity() {
        // given
        List<Account> accounts = new ArrayList<>();
        accounts.add(new Account("wasabi@yandex.ru", BigDecimal.valueOf(1000)));
        accounts.add(new Account("maxdadude@yandex.ru", BigDecimal.valueOf(1100)));
        when(accountRepository.findAll()).thenReturn(accounts);

        // when
        List<Account> result = accountService.getAllAccounts();

        // then
        assertNotNull(result);
        assertEquals("wasabi@yandex.ru", result.get(0).getEmail());
        assertEquals(BigDecimal.valueOf(1000), result.get(0).getBalance());
        assertEquals("maxdadude@yandex.ru", result.get(1).getEmail());
        assertEquals(BigDecimal.valueOf(1100), result.get(1).getBalance());

        verify(accountRepository, times(1)).findAll();
    }
}