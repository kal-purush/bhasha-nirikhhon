package com.dell.juliana.filme.dao;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by Juliana on 04/06/2017.
 */

public class FilmeDbHelper extends SQLiteOpenHelper {

    public static final String NOME_BANCO   = "dbFilme";
    public static final int VERSAO_BANCO    = 2;

    public static final String TABELA_FILME    = "filme";
    public static final String COL_ID          = "_id";
    public static final String COL_TITULO      = "titulo";
    public static final String COL_TIPO        = "tipo";
    public static final String COL_SINOPSE     = "sinopse";



    public FilmeDbHelper(Context ctx) {
        super(ctx, NOME_BANCO, null, VERSAO_BANCO);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE " + TABELA_FILME + "(" +
                COL_ID          +" INTEGER PRIMARY KEY AUTOINCREMENT," +
                COL_TITULO      +" TEXT NOT NULL," +
                COL_TIPO        +" TEXT NOT NULL," +
                COL_SINOPSE     +" TEXT NOT NULL," );
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
    }
}