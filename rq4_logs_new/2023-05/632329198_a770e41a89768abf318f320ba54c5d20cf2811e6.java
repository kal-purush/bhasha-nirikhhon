package ru.wasabi.my_atm.web.controller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.wasabi.my_atm.entity.account.Account;
import ru.wasabi.my_atm.service.AccountService;
import ru.wasabi.my_atm.service.impl.AccountServiceImpl;
import ru.wasabi.my_atm.web.dto.AccountDto;
import ru.wasabi.my_atm.web.mapper.AccountMapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AccountControllerTest {

    private static final Long ID = 1L;

    @Mock
    private AccountMapper accountMapper;

    @Mock
    private AccountService accountService;

    @InjectMocks
    private AccountController accountController;

    @Test
    void getAllAccounts() {

    }

    @Test
    void getAccountById() {
        // get
        final Account account = mock(Account.class);
        when(accountService.getAccountById(ID)).thenReturn(account);
        final AccountDto accountDto = mock(AccountDto.class);
        when(accountMapper.toDto(account)).thenReturn(accountDto);

        // when
        final AccountDto actual = accountController.getAccountById(ID);

        // then
        assertNotNull(actual);
        assertEquals(accountDto, actual);
        verify(accountService).getAccountById(ID);
        verify(accountMapper).toDto(account);
    }

    @Test
    void createAccount() {
        Account account = mock(Account.class);
        AccountDto accountDto = mock(AccountDto.class);
        when(accountMapper.toEntity(accountDto)).thenReturn(account);

        accountController.createAccount(accountDto);

        verify(accountMapper).toEntity(accountDto);
        verify(accountService).createAccount(account);
    }

    @Test
    void updateAccount() {
    }

    @Test
    void deleteAccount() {
    }
}