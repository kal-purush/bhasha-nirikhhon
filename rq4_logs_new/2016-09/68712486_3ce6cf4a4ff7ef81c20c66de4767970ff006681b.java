package com.evildice;

/**
 * Created by Vigi on 03/08/2016.
 */

import com.evildice.kiov.User;
import com.opensymphony.xwork2.ActionSupport;
import com.evildice.kiov.UserService;

import java.util.List;


public class AddArrayNrs extends ActionSupport {

    private String str;
    private Integer sum;
    private List<User> vser;

    public String calcSum() {
        String[] intArray = str.split(",");
        sum = 0;
        for (String s: intArray) {
            sum += Integer.parseInt(s);
        }
        UserService ust = new UserService();
        vser = ust.getAllUsers();
        return SUCCESS;
    }

    public String getStr() {
        return str;
    }
    public void setStr(String Str) {
        this.str = Str;
    }

    public Integer getSum() { return sum; }
    public void setSum(Integer Sum) {
        this.sum = Sum;
    }

    public List<User> getvser() {
        return vser;
    }
    public void setvser(List<User> vser) {
        this.vser = vser;
    }
}