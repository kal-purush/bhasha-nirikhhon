package com.willzcode;

import org.apache.commons.lang.enums.Enum;

import java.util.List;

/**
 * Created by willz on 2018/4/19.
 */
public class ForgeField {
    public String fieldName;
    public FieldType fieldType;
    public String value;
    public int value1;
    public int value2;
    public int base;
    public int add;
    public List<String> lores;
    public double start;
    public double bound;
    public int decimal;


    public enum FieldType {
        FIXED,
        RANDOM,
        STRENGTH,
        LORES,
        FLOAT,
    }
}