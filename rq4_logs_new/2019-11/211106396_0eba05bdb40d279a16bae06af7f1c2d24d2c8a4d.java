package com.example.emergensui.automotive_ui;

public class speedometer {
    private String speed;
    private String blood_pressure;
    private String heart_rate;
    private String oxygen;
    private String respiration;

    public speedometer(){

    }
    public speedometer(String speed, String blood_pressure,String heart_rate, String oxygen,String respiration) {
        this.speed= speed;
        this.blood_pressure= blood_pressure;
        this.heart_rate = heart_rate;
        this.oxygen= oxygen;
        this.respiration = respiration;
    }

    public String getSpeed(){ return speed; }
    public void setSpeed(String profession_id){
        this.speed= speed;
    }

    public String getHeart_rate(){ return heart_rate; }
    public void setHeart_rate(String heart_rate){
        this.heart_rate= heart_rate;
    }


    public String getBlood_pressure(){ return blood_pressure; }
    public void setBlood_pressure(String blood_pressure){
        this.blood_pressure=blood_pressure;
    }

    public String getOxygen(){ return oxygen; }
    public void setOxygen(String oxygen){
        this.oxygen= oxygen;
    }


    public String getRespiration(){ return respiration; }
    public void setRespiration(String respiration){
        this.respiration= respiration;
    }
}