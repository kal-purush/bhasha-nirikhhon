package ru.otus.java_2018_08.student.hw07.common;

import java.util.List;
import java.util.Objects;

public class Money {
    private int amount = 0;
    private Currency currency;

    public Money(Currency currency) {
        this.currency = currency;
    }

    public Money(Currency currency, int amount) {
        this(currency);
        this.amount = amount;
    }

    public Money(Banknote banknote) {
        this(banknote.getCurrency(), banknote.getNominal());
    }

    public Currency getCurrency() {
        return currency;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public boolean addBanknote(Banknote banknote) {
        if (banknote.getCurrency() != currency) {
            return false;
        }

        amount += banknote.getNominal();

        return true;
    }

    public boolean addMoney(Money money) {
        if (currency != money.getCurrency()) {
            return false;
        }

        amount += money.amount;

        return true;
    }

    static public Money createByBundle(List<Banknote> bundle) {
        Banknote banknote = bundle.get(0);
        if (banknote == null) {
            return null;
        }

        Money money = new Money(banknote);

        for (int i = 1; i < bundle.size(); i++) {
            if (!money.addBanknote(bundle.get(i))) {
                return null;
            }
        }

        return money;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Money money = (Money) o;
        return currency == money.currency;
    }

    @Override
    public int hashCode() {

        return Objects.hash(currency);
    }

    @Override
    public String toString() {
        return String.format("%d %s", amount, currency.getCurrencyView());
    }
}