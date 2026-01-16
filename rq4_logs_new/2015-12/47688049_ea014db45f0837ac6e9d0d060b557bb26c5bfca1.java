package com.yair.bookstore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by Yair on 20/12/2015.
 */

public class User {
    int _id;
    String _name;
    Calendar _birthday;

    public Calendar get_birthday() {
        return _birthday;
    }

    public String getBirthDateAsString() {
        DateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyyy");

        return dateFormatter.format(_birthday.getTime());
    }
    public String get_name() {
        return _name;
    }

    public int get_id() {
        return _id;
    }



}