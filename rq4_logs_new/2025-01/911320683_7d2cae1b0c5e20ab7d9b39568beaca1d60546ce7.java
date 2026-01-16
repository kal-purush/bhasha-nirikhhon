package com.sofkau.bank.commands;

import java.math.BigDecimal;

import com.sofkau.bank.entities.Account;

public class DepositCommand {
    private final Account destination;
    private final BigDecimal amount;

    private DepositCommand(Account destination, BigDecimal amount) {
        this.destination = destination;
        this.amount = amount;
    }

    public static class Builder {
        private Account account;

        private Builder(Account account) {
            this.account = account;
        }

        public DepositCommand with(BigDecimal amount) {
            return new DepositCommand(account, amount);
        }
    }

    public static Builder on(Account destination) {
        return new Builder(destination);
    }

    public Account getDestination() {
        return this.destination;
    }

    public BigDecimal getAmount() {
        return this.amount;
    }
}