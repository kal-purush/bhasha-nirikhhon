package com.yycg.base.bean;

public class AreaBackup {
    private String areaid;

    private String areacode;

    private String areaname;

    private String arealevel;

    private String areafullname;

    private String parentid;

    private String vchar1;

    private String vchar2;

    private String vchar3;

    private String isused;

    public String getAreaid() {
        return areaid;
    }

    public void setAreaid(String areaid) {
        this.areaid = areaid == null ? null : areaid.trim();
    }

    public String getAreacode() {
        return areacode;
    }

    public void setAreacode(String areacode) {
        this.areacode = areacode == null ? null : areacode.trim();
    }

    public String getAreaname() {
        return areaname;
    }

    public void setAreaname(String areaname) {
        this.areaname = areaname == null ? null : areaname.trim();
    }

    public String getArealevel() {
        return arealevel;
    }

    public void setArealevel(String arealevel) {
        this.arealevel = arealevel == null ? null : arealevel.trim();
    }

    public String getAreafullname() {
        return areafullname;
    }

    public void setAreafullname(String areafullname) {
        this.areafullname = areafullname == null ? null : areafullname.trim();
    }

    public String getParentid() {
        return parentid;
    }

    public void setParentid(String parentid) {
        this.parentid = parentid == null ? null : parentid.trim();
    }

    public String getVchar1() {
        return vchar1;
    }

    public void setVchar1(String vchar1) {
        this.vchar1 = vchar1 == null ? null : vchar1.trim();
    }

    public String getVchar2() {
        return vchar2;
    }

    public void setVchar2(String vchar2) {
        this.vchar2 = vchar2 == null ? null : vchar2.trim();
    }

    public String getVchar3() {
        return vchar3;
    }

    public void setVchar3(String vchar3) {
        this.vchar3 = vchar3 == null ? null : vchar3.trim();
    }

    public String getIsused() {
        return isused;
    }

    public void setIsused(String isused) {
        this.isused = isused == null ? null : isused.trim();
    }
}