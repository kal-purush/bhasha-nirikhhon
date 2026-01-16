package ru.wasabi.my_atm.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestBody;
import ru.wasabi.my_atm.entity.Account;
import ru.wasabi.my_atm.entity.exception.ResourceNotFoundException;
import ru.wasabi.my_atm.repository.AccountRepository;
import ru.wasabi.my_atm.service.AccountService;
import ru.wasabi.my_atm.web.dto.AccountDto;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class AccountServiceImpl implements AccountService {

    private final AccountRepository accountRepository;

    @Override
    @Transactional(readOnly = true)
    public List<Account> getAllAccounts() {
        return accountRepository.findAll();
    }

    @Override
    @Transactional(readOnly = true)
    public Account getAccountById(Long id) {
        return accountRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Аккаунт с таким id не найден: " + id));
    }

    @Override
    @Transactional(readOnly = true)
    public Account getAccountByEmail(String email) {
        return accountRepository.findByEmail(email)
                .orElseThrow(() -> new ResourceNotFoundException("Аккаунт с такой почтой не найден: " + email));
    }

    @Override
    @Transactional
    public Account createAccount(Account account) {
        if (account.getId() != null) {
            throw new IllegalStateException("У нового аккаунта не может быть id");
        }
        if (account.getBalance() != null) {
            throw new IllegalStateException("Нельзя задать стартовый баланс вручную");
        }
        account.setBalance(BigDecimal.valueOf(0));

        return accountRepository.save(account);
    }

    @Override
    @Transactional
    public Account update(Account account, String email) {
        Account existingAccount = accountRepository.findById(account.getId())
                .orElseThrow(() -> new ResourceNotFoundException("Account not found"));
        existingAccount.setId(account.getId());
        existingAccount.setEmail(account.getEmail());
        existingAccount.setBalance(account.getBalance());
        return accountRepository.save(existingAccount);
    }

    @Override
    @Transactional
    public void deleteAccountById(Long id) {
        Optional<Account> account = accountRepository.findById(id);
        if (account.isPresent()) {
            accountRepository.deleteById(id);
        } else {
            throw new ResourceNotFoundException("Аккаунт с таким номером не найден: " + id);
        }
    }

    @Override
    public Long getId(Long id) {
        Account existingAccount = accountRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException("Account not found"));
        return existingAccount.getId();
    }

    @Override
    public BigDecimal getBalance(Long id) {
        Account existingAccount = accountRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException("Account not found"));
        return existingAccount.getBalance();
    }

}