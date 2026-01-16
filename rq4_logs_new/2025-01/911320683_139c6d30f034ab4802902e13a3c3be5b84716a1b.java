package com.sofkau.bank.factories;

import java.util.Map;

import org.springframework.stereotype.Service;

import com.sofkau.bank.commands.DepositCommand;
import com.sofkau.bank.commands.OperationalCommand;
import com.sofkau.bank.commands.TransferCommand;
import com.sofkau.bank.commands.WithdrawalCommand;
import com.sofkau.bank.constants.OperationTypes;
import com.sofkau.bank.services.operations.OperationService;

@Service
public class OperationServiceFactory {
    private final Map<String, OperationService<? extends OperationalCommand>> operations;

    public OperationServiceFactory(Map<String, OperationService<? extends OperationalCommand>> operations) {
        this.operations = operations;
    }

    @SuppressWarnings("unchecked")
    public <T extends OperationalCommand> OperationService<T> from(T command) {
        String type = "";

        if (command instanceof DepositCommand)
            type = OperationTypes.DEPOSIT;

        if (command instanceof WithdrawalCommand)
            type = OperationTypes.WITHDRAWAL;

        if (command instanceof TransferCommand)
            type = OperationTypes.TRANSFER;

        return (OperationService<T>) operations.getOrDefault(type, null);
    }
}