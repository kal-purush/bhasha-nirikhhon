package com.project.symptoms;

public class Pressure{

    private int pressure_id;
    private int systolic;
    private int diastolic;
    private String date;
    private String time;

    public Pressure(int pressure_id, int systolic, int diastolic, String date, String time){
        this.pressure_id = pressure_id;
        this.systolic = systolic;
        this.diastolic = diastolic;
        this.date = date;
        this.time = time;
    }

    public Pressure(int systolic, int diastolic, String date, String time){
        this(0, systolic, diastolic, date, time);
    }


    public int getPressure_id() {
        return pressure_id;
    }

    public void setPressure_id(int pressure_id) {
        this.pressure_id = pressure_id;
    }

    public int getSystolic() {
        return systolic;
    }

    public void setSystolic(int systolic) {
        this.systolic = systolic;
    }

    public int getDiastolic() {
        return diastolic;
    }

    public void setDiastolic(int diastolic) {
        this.diastolic = diastolic;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}