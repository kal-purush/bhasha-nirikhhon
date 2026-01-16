package com.arbaini.getih.Model;

public class Users {

    private String UserId,NoHp,GolDar,BeratBadan,Email;

    public Users(String userId, String noHp, String golDar, String beratBadan, String email) {
        UserId = userId;
        NoHp = noHp;
        GolDar = golDar;
        BeratBadan = beratBadan;
        Email = email;
    }

    public String getUserId() {
        return UserId;
    }

    public void setUserId(String userId) {
        UserId = userId;
    }

    public String getNoHp() {
        return NoHp;
    }

    public void setNoHp(String noHp) {
        NoHp = noHp;
    }

    public String getGolDar() {
        return GolDar;
    }

    public void setGolDar(String golDar) {
        GolDar = golDar;
    }

    public String getBeratBadan() {
        return BeratBadan;
    }

    public void setBeratBadan(String beratBadan) {
        BeratBadan = beratBadan;
    }

    public String getEmail() {
        return Email;
    }

    public void setEmail(String email) {
        Email = email;
    }
}