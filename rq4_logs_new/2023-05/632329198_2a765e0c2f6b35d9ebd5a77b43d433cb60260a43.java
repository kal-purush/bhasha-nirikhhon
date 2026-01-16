package ru.wasabi.my_atm.web.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.wasabi.my_atm.entity.exception.ResourceNotFoundException;
import ru.wasabi.my_atm.entity.transaction.TransactionHistory;
import ru.wasabi.my_atm.service.TransactionHistoryService;
import ru.wasabi.my_atm.web.dto.TransactionHistoryDto;
import ru.wasabi.my_atm.web.mapper.TransactionHistoryMapper;

import java.util.List;

@RestController
@RequestMapping("/api/v1/operation-history")
@RequiredArgsConstructor
public class TransactionHistoryController {

    private final TransactionHistoryService transactionHistoryService;
    private final TransactionHistoryMapper transactionHistoryMapper;

    @GetMapping("/{accountId}")
    public List<TransactionHistoryDto> getOperationList(@PathVariable Long accountId) {
        List<TransactionHistory> transactionHistoryList = transactionHistoryService.getOperationListByAccountId(accountId);
        if (transactionHistoryList.isEmpty()) {
            throw new ResourceNotFoundException("На вашем аккаунте нет транзакций");
        }
        return transactionHistoryMapper.toDto(transactionHistoryList);
    }

}