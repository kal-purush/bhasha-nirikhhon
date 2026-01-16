package com.mycompany.myapplication;

/**
 * Created by 7 on 2/14/2015.
 */
public class DataBaseManager {
    public static final String TABLE_NAME = "tarea";

    public static final String CN_ID = "_id";
    public static final String CN_NAME = "nombre";
    public static final String CN_DESC = "descripcion";

    public static final String CREATE_TABLE = " create table " + TABLE_NAME +" ("
            + CN_ID + " integer priamry key autoincrement,"
            + CN_NAME + " text not null,"
            + CN_DESC + " text);";



}