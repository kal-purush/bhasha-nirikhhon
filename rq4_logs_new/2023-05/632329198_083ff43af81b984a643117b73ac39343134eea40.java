package ru.wasabi.my_atm.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.wasabi.my_atm.entity.exception.ResourceNotFoundException;
import ru.wasabi.my_atm.entity.transaction.TransactionHistory;
import ru.wasabi.my_atm.repository.TransactionHistoryRepository;
import ru.wasabi.my_atm.service.TransactionHistoryService;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TransactionHistoryServiceImpl implements TransactionHistoryService {

    private final TransactionHistoryRepository transactionHistoryRepository;

    @Override
    public void save(TransactionHistory history) {
        transactionHistoryRepository.save(history);
    }

    @Override
    public List<TransactionHistory> getByAccountId(Long accountId) {
        return transactionHistoryRepository.findByAccount_Id(accountId);
    }
}