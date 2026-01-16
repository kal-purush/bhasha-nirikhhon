package com.datamasking.helperClasses;

import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;

public class KAnonymityRequestBody {
    MultipartFile xmlFile;
    Integer k;
    ArrayList<String> xPaths;

    public KAnonymityRequestBody(MultipartFile xmlFile, Integer k, ArrayList<String> xPaths) {
        this.xmlFile = xmlFile;
        this.k = k;
        this.xPaths = xPaths;
    }

    public MultipartFile getXmlFile() {
        return xmlFile;
    }

    public void setXmlFile(MultipartFile xmlFile) {
        this.xmlFile = xmlFile;
    }

    public Integer getK() {
        return k;
    }

    public void setK(Integer k) {
        this.k = k;
    }

    public ArrayList<String> getxPaths() {
        return xPaths;
    }

    public void setxPaths(ArrayList<String> xPaths) {
        this.xPaths = xPaths;
    }
}