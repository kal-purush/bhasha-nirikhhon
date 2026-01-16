package org.jamaica.pcog.mobile.adapter;

/**
 * Created by Sweety on 04-04-2016.
 */
// STEP 4.1
    // CREATE SEPERTAE CLASS WITH REQUIRED VARIABLE THAT WE WANT TO STORE DATA DEPENDING UPON JSON S
public class Events {

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public String getMonth() {
        return month;
    }
    public void setMonth(String month) {
        this.month = month;
    }

    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }

    public String getEmail() {
        return email;
    }
    public void setEmail(String email) {
        this.email = email;
    }

    public String getDay() {
        return day;
    }
    public void setDay(String day) {
        this.day = day;
    }

    private String name,email,day, month, description;
}