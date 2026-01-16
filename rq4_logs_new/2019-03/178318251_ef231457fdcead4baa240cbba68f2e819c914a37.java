package com.imp.sorm.bean;

/**
 * 封装一个字段的类
 * @author Imp
 * email: 1318944013@qq.com
 * date: 2018/9/24 21:17
 */
public class ColumnInfo {

    private String name; // 字段名

    private String dataType; // 数据类型

    private int keyType; // 0:普通键 1:主键 2:外键

    public ColumnInfo(String name, String dataType, int keyType) {
        this.name = name;
        this.dataType = dataType;
        this.keyType = keyType;
    }

    public  ColumnInfo() {};

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getKeyType() {
        return keyType;
    }

    public void setKeyType(int keyType) {
        this.keyType = keyType;
    }
}