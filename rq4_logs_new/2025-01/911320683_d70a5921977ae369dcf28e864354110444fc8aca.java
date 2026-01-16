package com.sofkau.bank.services;

import com.sofkau.bank.entities.Account;
import com.sofkau.bank.entities.Client;
import com.sofkau.bank.exceptions.AlreadyExistsException;
import com.sofkau.bank.exceptions.NotFoundException;
import com.sofkau.bank.repositories.accounts.AccountRepository;
import com.sofkau.bank.repositories.accounts.AccountTypeRepository;
import com.sofkau.bank.services.accounts.AccountServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AccountServiceTest {
    @Mock
    private AccountRepository accountRepository;

    @Mock
    private AccountTypeRepository accountTypeRepository;

    @InjectMocks
    private AccountServiceImpl accountService;

    private final UUID sampleAccountNumber = UUID.randomUUID();

    @Test
    void whenCreateAccountWithDataAccountGetsSavedThenReturnAccount() {
        // Arrange
        Account sample = new Account();
        sample.setClient(new Client());

        when(accountRepository.save(any(Account.class)))
                .thenReturn(sample);

        // Act & assert

        Account result = assertDoesNotThrow(() -> accountService.createAccount(sample));

        verify(accountRepository, times(1))
                .save(any(Account.class));

        assertNotNull(result);
    }

    @Test
    void whenCreateAccountWithNoClientThenThrowsException() {
        // Act & assert
        assertThrows(NotFoundException.class, () -> accountService.createAccount(new Account()));

        verify(accountRepository, never())
                .save(any(Account.class));
    }

    @Test
    void whenCreateAccountWithIdGreaterThanZeroThenThrowsException() {
        // Arrange
        Account sample = new Account();
        sample.setId(1);
        sample.setClient(new Client());

        // Act & assert
        assertThrows(AlreadyExistsException.class, () -> accountService.createAccount(sample));

        verify(accountRepository, never())
                .save(any(Account.class));
    }

    @Test
    void whenUpdateAccountGetsAccountUpdatedThenDoesNotReturn() {
        // Arrange
        Account storedAccount = spy(new Account());

        Account accountForUpdate = new Account();
        accountForUpdate.setBalance(BigDecimal.valueOf(40));

        when(accountRepository.findByNumber(any(UUID.class)))
                .thenReturn(Optional.of(storedAccount));

        // Act & assert
        assertDoesNotThrow(() -> accountService.updateAccount(sampleAccountNumber, accountForUpdate));

        verify(storedAccount, times(1))
                .setBalance(any(BigDecimal.class));

        verify(accountRepository, times(1))
                .findByNumber(any(UUID.class));

        verify(accountRepository, times(1))
                .save(eq(storedAccount));
    }

    @Test
    void whenFindAccountByNumberDoesNotFindAccountThenThrowsException() {
        // Arrange
        when(accountRepository.findByNumber(any(UUID.class)))
                .thenReturn(Optional.empty());

        // Act & assert
        assertThrows(NotFoundException.class, () -> accountService.findAccountByNumber(sampleAccountNumber));
    }

    @Test
    void whenFindAccountTypeByNameGetsAccountTypeThenReturnsAccountType() {
        // Arrange
        Account.Type.Name sampleName = Account.Type.Name.CHECKING;

        when(accountTypeRepository.findByName(anyString()))
                .thenReturn(Optional.of(new Account.Type()));

        // Act & assert
        Account.Type result = assertDoesNotThrow(() -> accountService.findAccountTypeByName(sampleName));

        verify(accountTypeRepository, times(1))
                .findByName(eq(sampleName.name()));

        assertNotNull(result);
    }

    @Test
    void whenFindAccountTypeByNameDoesNotFindAccountThenThrowsException() {
        // Arrange
        Account.Type.Name sampleName = Account.Type.Name.CHECKING;

        when(accountTypeRepository.findByName(anyString()))
                .thenReturn(Optional.empty());

        // Act & assert
        assertThrows(NotFoundException.class, () -> accountService.findAccountTypeByName(sampleName));

        verify(accountTypeRepository, times(1))
                .findByName(eq(sampleName.name()));
    }

    @Test
    void whenAccountBelongsToClientThenReturnBoolean() {
        // Arrange
        when(accountRepository.existsByNumberAndClient(any(UUID.class), any(Client.class)))
                .thenReturn(true);

        // Act & assert
        boolean result = assertDoesNotThrow(() -> accountService
                .accountBelongsToClient(sampleAccountNumber, new Client()));

        assertTrue(result);
    }

    @Test
    void whenFindClientAccountsFindsAccountsThenReturnList() {
        // Arrange
        List<Account> sampleAccounts = List.of(new Account(), new Account());

        when(accountRepository.findAllByClient(any(Client.class)))
                .thenReturn(sampleAccounts);

        // Act & assert
        List<Account> result = assertDoesNotThrow(() -> accountService.findClientAccounts(new Client()));

        assertNotNull(result);
        assertEquals(sampleAccounts.size(), result.size());
    }
}