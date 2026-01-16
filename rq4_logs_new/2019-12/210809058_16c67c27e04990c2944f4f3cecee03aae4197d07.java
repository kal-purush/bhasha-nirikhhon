package ru.otus.atmdepartment.atm;

public class BalanceInfoImpl implements BalanceInfo {

    int totalMoney;
    int totalBanknotes;

    public BalanceInfoImpl(){}

    public BalanceInfoImpl(int totalMoney, int totalBanknotes) {
        this.totalMoney = totalMoney;
        this.totalBanknotes = totalBanknotes;
    }

    public int getTotalMoney() {
        return totalMoney;
    }

    public int getTotalBanknotes() {
        return totalBanknotes;
    }

    public void setTotalMoney(int totalMoney) {
        this.totalMoney = totalMoney;
    }

    public void setTotalBanknotes(int totalBanknotes) {
        this.totalBanknotes = totalBanknotes;
    }
}