package com.datamasking.helperClasses;

import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.Collections;

public class NumericGeneralizationRequestBody {
    MultipartFile xmlFile;
    ArrayList<String> xpaths;
    ArrayList<Integer> rangeMax;
    Integer k;

    public Integer getK() {
        return k;
    }

    public void setK(Integer k) {
        this.k = k;
    }

    public NumericGeneralizationRequestBody() {
    }

    public NumericGeneralizationRequestBody(MultipartFile xmlFile, ArrayList<String> xpaths, ArrayList<Integer> rangeMax, Integer k) {
        this.xmlFile = xmlFile;
        this.xpaths = xpaths;
        this.rangeMax = rangeMax;
        this.k = k;
    }

    public MultipartFile getXmlFile() {
        return xmlFile;
    }

    public void setXmlFile(MultipartFile xmlFile) {
        this.xmlFile = xmlFile;
    }

    public ArrayList<String> getXpaths() {
        return xpaths;
    }

    public void setXpaths(ArrayList<String> xpaths) {
        this.xpaths = xpaths;
    }

    public ArrayList<Integer> getRangeMax() {
        return rangeMax;
    }

    public void setRangeMax(ArrayList<Integer> rangeMax) {
        this.rangeMax = rangeMax;
    }
}