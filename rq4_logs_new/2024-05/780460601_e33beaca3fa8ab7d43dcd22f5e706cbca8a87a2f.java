package com.hishamfactory;

import java.util.ArrayList;

public class LoginHistory {
    private String date_and_time;
    private Person user;
    public  static ArrayList<LoginHistory> login_history = new ArrayList<>();
    public LoginHistory(){}
    public LoginHistory(String date_and_time,Person user){
        this.date_and_time = date_and_time;
        this.user = user;
        System.out.println(this.getUser().getFirst_name());
        login_history.add(this);
    }
    public void printLoginHistory(){
        System.out.println("..............................Login History.............................");
        System.out.println("    Date and Time       |       User");
        for (LoginHistory history : login_history) {
        System.out.println(" " +history.getDate_and_time()+"     |      "+history.getUser().getFirst_name()+" "+history.getUser().getLast_name());
        }
    }
    public String getDate_and_time() {
        return date_and_time;
    }
    public Person getUser() {
        return user;
    }

}