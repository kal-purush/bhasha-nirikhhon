package com.giovas.data;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.util.ArrayList;

/**
 * Created by DarkGeat on 10/28/2015.
 */
public class DataBase {

    private static final String nTZipcodes = "ZipCodes";
    private static final String DataName = "MyData";
    private static final String Key_idZipcodes = "ID_Zipcode";
    private static final String Key_zipcode = "Zipcode";
    private static final int myVersion = 1;
    private static final String Tzipcodes = "CREATE TABLE " + nTZipcodes +
            " (" + Key_idZipcodes + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
            Key_zipcode + " TEXT NOT NULL);";
    private MyHelper myDB;
    private SQLiteDatabase myDataBase;
    private final Context ourContext;

    public DataBase(Context c){
        ourContext = c;
    }

    private static class MyHelper extends SQLiteOpenHelper {

        public MyHelper(Context context) {
            super(context, DataName, null, myVersion);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(Tzipcodes);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " + nTZipcodes );
            onCreate(db);
            db.setVersion(newVersion);
        }
    }
    /**
     * Abre la Base de Datos.
     * @return La base de datos Abierta.
     */
    public DataBase open(){
        myDB = new MyHelper(ourContext);
        myDataBase = myDB.getWritableDatabase();
        return this;
    }

    /**
     * Cierra la base de Datos.
     */
    public void close(){
        myDB.close();
        myDataBase.close();
    }

    /**
     * Este metodo es para la entrada a una tabla generica que necesite solo "latitud", "longitud" y  "tiempo".
     * @param zipcode Cadena que representa la latitud que se Ingresara.
     */
    public void newEntryZipcodes(String zipcode){
        open();
        ContentValues registro = new ContentValues();
        registro.put(Key_zipcode, zipcode);
        myDataBase.insert(nTZipcodes, null, registro);
        close();
    }

    /**
     * Este mï¿½todo regresa los datos completos de la Tabla de Conductores.
     * @return Regresa un Vector de tamaï¿½o 3, en el vector con indice 0 se guardan las latitudes de localizacion,
     * en el vector con indice 1 se guardan las longitudes de localizacion, y por ultimo en el vector con indice
     * 2 se guardan los timepos en lo hubo la actividad.IMPORTANTE: Para que este metodo Funcione se debe de crear el
     * arreglo de vectores de manera que espere recibir String's u objetos.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ArrayList<String> getZipcodes(){
        open();
        String[] columnas = new String[]{Key_idZipcodes,Key_zipcode};
        Cursor puntero = myDataBase.query(nTZipcodes, columnas,null,null,null,null,null);
        ArrayList<String> zipcodesindb = new ArrayList<>();
        for(puntero.moveToFirst();!puntero.isAfterLast();puntero.moveToNext()){
            zipcodesindb.add(puntero.getString(puntero.getColumnIndex(Key_zipcode)));
        }
        close();
        return zipcodesindb;
    }

    public void removeZipcode(String zipcodeToRemove){
        open();
        myDataBase.delete(nTZipcodes,Key_zipcode + "=" + zipcodeToRemove,null);
        close();
    }


    /*	public boolean IsEmptyEstado(){
        try{
            open();
            Cursor c = myDataBase.query(nTEstado, new String[]{Key_comienzo}, null, null,null,null,null);
            if(c.moveToFirst()){
                return false;}
            else
                return true;
        }catch(Exception e){
            return true;
        }finally{
            close();
        }

    }

    public boolean IsEmptyService(){
        try{
            open();
            Cursor c = myDataBase.query(nTEstadoService, new String[]{Key_apagado}, null, null,null,null,null);
            if(c.moveToFirst()){
                return false;}
            else
                return true;
        }catch(Exception e){
            return true;
        }finally{
            close();
        }

    }

    public boolean IsEmptyLocacionInicial(){
        try{
            open();
            Cursor c = myDataBase.query(nTLocInicial, new String[]{Key_latinicial}, null, null,null,null,null);
            if(c.moveToFirst()){
                return false;}
            else
                return true;
        }catch(Exception e){
            return true;
        }finally{
            close();
        }
    }

    public boolean IsEmptyLocacionFinal(){
        try{
            open();
            Cursor c = myDataBase.query(nTLocFinal, new String[]{Key_latfinal}, null, null,null,null,null);
            if(c.moveToFirst()){
                return false;}
            else
                return true;
        }catch(Exception e){
            return true;
        }finally{
            close();
        }
    }

    public boolean IsEmptyLocacionInnusual(){
        try{
            open();
            Cursor c = myDataBase.query(nTLocInnusual, new String[]{Key_latinusual}, null, null,null,null,null);
            if(c.moveToFirst()){
                return false;}
            else
                return true;
        }catch(Exception e){
            return true;
        }finally{
            close();
        }
    }

	/*
	public void BorraBaseDeDatos(){
		open();
		myDataBase.execSQL("DROP TABLE IF EXISTS " + nTEstado );
		myDataBase.execSQL("DROP TABLE IF EXISTS " + nTHistorial );
		myDataBase.execSQL("DROP TABLE IF EXISTS " + nTLogin );
		close();
	}

	public void DeleteTable(String Table){
		open();
		myDataBase.execSQL("DROP TABLE IF EXISTS " + Table);
		if(Table == nTEstado){
			myDataBase.execSQL(Testado);
		}else if(Table == nTHistorial){
			myDataBase.execSQL(Thistorial);
		}else if(Table == nTLogin){
			myDataBase.execSQL(TLogin);
		}else if(Table == nTEstadoService){
			myDataBase.execSQL(TEstadoService);
		}else if(Table == nTLocInicial){
			myDataBase.execSQL(TLocacionInicial);
		}else if(Table == nTLocFinal){
			myDataBase.execSQL(TLocacionFinal);
		}else{
			myDataBase.execSQL(TLocacionInnusual);
		}
		close();
	}


	public static String getNtEstado() {
		return nTEstado;
	}

	public static String getNtHistorial() {
		return nTHistorial;
	}

	public static String getNtLogin() {
		return nTLogin;
	}

	public static String getNtLocacionInicial() {
		return nTLocInicial;
	}

	public static String getNtLocacionFinal() {
		return nTLocFinal;
	}

	public static String getNtLocacionInnusual() {
		return nTLocInnusual;
	}

	public static String getNtService() {
		return nTEstadoService;
	}
	public static String getDataname() {
		return DataName;
	}*/

}