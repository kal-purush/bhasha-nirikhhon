package ru.otus.homework.dao;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public abstract class MetaInfo {
    private static Class thisClass;

    private static String idField;
    private static List<String> classFields;
    private static String insertCommand;
    private static String updateCommand;
    private static String selectCommand;

    protected static void fillMetaInfo(Class<? extends MetaInfo> clazz) {
        thisClass = clazz;
        idField = setIdFieldOnTable();
        classFields = fillClassFields();
        insertCommand = setInsertCommand();
        updateCommand = setUpdateCommand();
        selectCommand = setSelectCommand();
    }

    public static String getIdField() {
        return idField;
    }

    public static List<String> getClassFields() {
        return classFields;
    }

    public static String getInsertCommand() {
        return insertCommand;
    }

    public static String getUpdateCommand() {
        return updateCommand;
    }

    public static String getSelectCommand() {
        return selectCommand;
    }

    private static String setIdFieldOnTable() {
        for (Field field : thisClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Id.class)) {
                return field.getName();
            }
        }
        return null;
    }

    private static List<String> fillClassFields() {
        List classFields = new ArrayList();
        for (Field field : thisClass.getDeclaredFields()) {
            if (!field.getName().equals(idField)) {
                classFields.add(field.getName());
            }
        }
        return classFields;
    }

    private static String setInsertCommand() {
        if (classFields.size() > 1) {
            StringBuilder listColumnNames = new StringBuilder();
            StringBuilder listParameters = new StringBuilder();
            for (String field : classFields) {
                if (!field.equals(idField)) {
                    listColumnNames.append(field);
                    listColumnNames.append(",");
                    listParameters.append("?,");
                }
            }
            listColumnNames.setLength(listColumnNames.length() - 1);
            listParameters.setLength(listParameters.length() - 1);
            return String.format("insert into %s(%s) values (%s)", thisClass.getSimpleName(), listColumnNames.toString(), listParameters.toString());
        } else {
            return null;
        }
    }

    private static String setUpdateCommand() {
        if (thisClass.getDeclaredFields().length > 1) {
            StringBuilder listColumnNames = new StringBuilder();
            for (String field : classFields) {
                if (!field.equals(idField)) {
                    listColumnNames.append(field);
                    listColumnNames.append("=?,");
                }
            }
            listColumnNames.setLength(listColumnNames.length() - 1);
            return String.format("update %s set %s where %s=?", thisClass.getSimpleName(), listColumnNames.toString(), idField);
        } else {
            return null;
        }
    }

    private static String setSelectCommand() {
        return String.format("select * from %s where %s = ?", thisClass.getSimpleName(), idField);
    }
}