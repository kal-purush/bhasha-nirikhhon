package edu.ulatina.objects;

public class SiteTO {
    
    private int id;
    private String name;
    private String province;
    private String canton;
    private String adress;
    private int phone;
    private String state;

    public SiteTO() {
    }

    public SiteTO(int id, String name, String province, String canton, String adress, int phone, String state) {
        this.id = id;
        this.name = name;
        this.province = province;
        this.canton = canton;
        this.adress = adress;
        this.phone = phone;
        this.state = state;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCanton() {
        return canton;
    }

    public void setCanton(String canton) {
        this.canton = canton;
    }

    public String getAdress() {
        return adress;
    }

    public void setAdress(String Adress) {
        this.adress = Adress;
    }

    public int getPhone() {
        return phone;
    }

    public void setPhone(int phone) {
        this.phone = phone;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}