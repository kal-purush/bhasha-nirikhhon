package com.future.utils;

/**
 * Created by liuyangyang on 2017/2/22.
 */
public class Balance {

    public String union;
    public double income;//收入
    public double expend;//支出
    public double balance;//余额

    public double getBalance() {
        return balance;
    }

    public void setBalance(double balance) {
        this.balance = balance;
    }

    public String getUnion() {
        return union;
    }

    public void setUnion(String union) {
        this.union = union;
    }

    public double getIncome() {
        return income;
    }

    public void setIncome(double income) {
        this.income = income;
    }

    public double getExpend() {
        return expend;
    }

    public void setExpend(double expend) {
        this.expend = expend;
    }
}