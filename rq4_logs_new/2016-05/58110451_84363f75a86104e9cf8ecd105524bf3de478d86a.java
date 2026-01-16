package com.example.administrator.smallvault.db.entity;

/**
 * Created by Administrator on 2016/5/10.
 */
public class SiFangQian {

    public String getTime() {
        return time;
    }

    public SiFangQian setTime(String time) {
        this.time = time;
        return this;
    }

    public String getMoney() {
        return money;
    }

    public SiFangQian setMoney(String money) {
        this.money = money;
        return this;
    }

    public String getPaywhere() {
        return paywhere;
    }

    public SiFangQian setPaywhere(String paywhere) {
        this.paywhere = paywhere;
        return this;
    }

    private String time;
    private String money;
    private String paywhere;
}