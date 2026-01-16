package com.datamasking.helperClasses;

import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;

public class TclosenessRequestBody {

    MultipartFile xmlFile;
    Integer k;
    Integer t;
    ArrayList<String> xPaths;
    String sas; //sensitive attributes

    public void setXmlFile(MultipartFile xmlFile) {
        this.xmlFile = xmlFile;
    }

    public TclosenessRequestBody(MultipartFile xmlFile, Integer k, Integer t, ArrayList<String> xPaths, String sas) {
        this.xmlFile = xmlFile;
        this.k = k;
        this.t = t;
        this.xPaths = xPaths;
        this.sas = sas;
    }

    @Override
    public String toString() {
        return "TclosenessRequestBody{" +
                "xmlFile=" + xmlFile +
                ", k=" + k +
                ", t=" + t +
                ", xPaths=" + xPaths +
                ", sas='" + sas + '\'' +
                '}';
    }

    public void setK(Integer k) {
        this.k = k;
    }

    public void setT(Integer t) {
        this.t = t;
    }

    public void setxPaths(ArrayList<String> xPaths) {
        this.xPaths = xPaths;
    }

    public void setSas(String sas) {
        this.sas = sas;
    }

    public MultipartFile getXmlFile() {
        return xmlFile;
    }

    public Integer getK() {
        return k;
    }

    public Integer getT() {
        return t;
    }

    public ArrayList<String> getxPaths() {
        return xPaths;
    }

    public String getSas() {
        return sas;
    }
}