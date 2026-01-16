package app.entities;

public class Emprunteur {

    private String username;

    private int memberShip;

    private String cin;



    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getMemberShip() {
        return memberShip;
    }

    public void setMemberShip(int memberShip) {
        this.memberShip = memberShip;
    }

    public String getCin() {
        return cin;
    }

    public void setCin(String cin) {
        this.cin = cin;
    }
}