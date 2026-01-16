package es.gameapp.multigameapp.extras;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import java.util.ArrayList;

import modelo.Tarea;
import modelo.Usuario;

/**
 * Wraps the logic for a SQLite database
 */
public class DataManager extends SQLiteOpenHelper {

    // Database Information
    private static final String DB_NAME = "agendaTareas.db";

    // Database version
    private static final int DB_VERSION = 1;

    // Table Name
    public static final String TABLE_NAME_USUARIO = "Usuario";

    // Table columns
    private static final String ID_USUARIO = "id";
    private static final String NOMBRE = "nombre";
    private static final String PASSWORD = "password";


    // Creating table query
    private static final String CREATE_TABLE_USUARIOS = "create table " + TABLE_NAME_USUARIO + "(" +
            ID_USUARIO + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
            NOMBRE + " TEXT, " +
            PASSWORD + " TEXT " +
            ");";

    // Table Name
    public static final String TABLE_NAME_TAREA = "Tarea";

    // Table columns
    private static final String ID_TAREA = "id";
    private static final String NOMBRE_TAREA = "nombre";
    private static final String DESCRIPCION = "descripcion";
    private static final String FECHA = "fecha";
    private static final String COSTE = "coste";
    private static final String PRIORIDAD = "prioridad";
    private static final String REALIZADO = "realizado";
    private static final String ID_USUARIO_FK = "id_usuario";

    private static final String CREATE_TABLE_TAREAS = "create table " + TABLE_NAME_TAREA + "(" +
            ID_TAREA + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
            ID_USUARIO_FK + " TEXT, " +
            NOMBRE_TAREA + " TEXT, " +
            DESCRIPCION + " TEXT, " +
            FECHA + " TEXT, " +
            COSTE + " TEXT, " +
            PRIORIDAD + " TEXT, " +
            REALIZADO + " INTEGER, " +
            " FOREIGN KEY ("+ID_USUARIO_FK+") REFERENCES "+TABLE_NAME_USUARIO+"("+ID_USUARIO+"));";

    private final Context context;

    public DataManager(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
        this.context = context;
    }

    @Override
    public void onCreate(SQLiteDatabase sQLiteDatabase) {
        sQLiteDatabase.execSQL(CREATE_TABLE_USUARIOS);
        sQLiteDatabase.execSQL(CREATE_TABLE_TAREAS);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sQLiteDatabase, int oldVersion, int newVersion) {
        sQLiteDatabase.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME_TAREA);
        sQLiteDatabase.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME_USUARIO);
        onCreate(sQLiteDatabase);
    }

    //------------------------------ selectAllUsuarios ------------------------------//

    public ArrayList<Usuario> selectAllUsuarios () {
        ArrayList<Usuario> ret = new ArrayList<>();
        String query = "SELECT * FROM " + TABLE_NAME_USUARIO;
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sQLiteDatabase.rawQuery(query, null);
        Usuario usuario;
        while (cursor.moveToNext()) {
            usuario = new Usuario();
            usuario.setId(cursor.getInt(0));
            usuario.setNombreUsuario(cursor.getString(1));
            usuario.setPassword(cursor.getString(2));
            ret.add(usuario);
        }
        cursor.close();
        sQLiteDatabase.close();
        return ret;
    }

    //------------------------------ selectAllTareas ------------------------------//

    public ArrayList<Tarea> selectAllTareas () {
        ArrayList<Tarea> ret = new ArrayList<>();
        String query = "SELECT * FROM " + TABLE_NAME_TAREA;
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sQLiteDatabase.rawQuery(query, null);
        Tarea tarea;
        while (cursor.moveToNext()) {
            tarea = new Tarea();
            tarea.setId(cursor.getInt(0));
            tarea.setIdUsuarioFk(cursor.getInt(1));
            tarea.setNombreTarea(cursor.getString(2));
            tarea.setDescripcion(cursor.getString(3));
            tarea.setFecha(cursor.getString(4));
            tarea.setCoste(Integer.parseInt(cursor.getString(5)));
            tarea.setPrioridad(cursor.getString(6));
            tarea.setRealizado(Integer.parseInt(cursor.getString(7)));

            ret.add(tarea);
        }
        cursor.close();
        sQLiteDatabase.close();
        return ret;
    }

    //------------------------------ selectAllTareasByUsuario ------------------------------//

    public ArrayList<Tarea> selectAllTareasByUsuario (int idUsuario) {
        ArrayList<Tarea> ret = new ArrayList<>();
        String query = "Select * FROM " + TABLE_NAME_TAREA + " WHERE " + ID_TAREA +
                " = " + "'" + ID_USUARIO_FK + "'";
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sQLiteDatabase.rawQuery(query, null);
        Tarea tarea;
        while (cursor.moveToNext()) {
            tarea = new Tarea();
            tarea.setId(cursor.getInt(0));
            tarea.setIdUsuarioFk(cursor.getInt(1));
            tarea.setNombreTarea(cursor.getString(2));
            tarea.setDescripcion(cursor.getString(3));
            tarea.setFecha(cursor.getString(4));
            tarea.setCoste(Integer.parseInt(cursor.getString(5)));
            tarea.setPrioridad(cursor.getString(6));
            tarea.setRealizado(Integer.parseInt(cursor.getString(7)));

            ret.add(tarea);
        }
        cursor.close();
        sQLiteDatabase.close();
        return ret;
    }

    //------------------------------ selectAllTareasRealizadasByUsuario ------------------------------//

    public ArrayList<Tarea> selectAllTareasRealizadasByUsuario (int idUsuario) {
        ArrayList<Tarea> ret = new ArrayList<>();
        String query = "Select * FROM " + TABLE_NAME_TAREA + " WHERE " + ID_USUARIO_FK +
                "=" + "'" + idUsuario + "'" + " and " + REALIZADO + "='1'";
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sQLiteDatabase.rawQuery(query, null);
        Tarea tarea;
        while (cursor.moveToNext()) {
            tarea = new Tarea();
            tarea.setId(cursor.getInt(0));
            tarea.setIdUsuarioFk(cursor.getInt(1));
            tarea.setNombreTarea(cursor.getString(2));
            tarea.setDescripcion(cursor.getString(3));
            tarea.setFecha(cursor.getString(4));
            tarea.setCoste(Integer.parseInt(cursor.getString(5)));
            tarea.setPrioridad(cursor.getString(6));
            tarea.setRealizado(Integer.parseInt(cursor.getString(7)));

            ret.add(tarea);
        }
        cursor.close();
        sQLiteDatabase.close();
        return ret;
    }

    //------------------------------ selectAllTareasPendientesByUsuario ------------------------------//

    public ArrayList<Tarea> selectAllTareasPendientesByUsuario (int idUsuario) {
        ArrayList<Tarea> ret = new ArrayList<>();
        String query = "Select * FROM " + TABLE_NAME_TAREA + " WHERE " + ID_USUARIO_FK +
                " = " + "'" + idUsuario + "'" + " and " +  REALIZADO +  "='0'" ;
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sQLiteDatabase.rawQuery(query, null);
        Tarea tarea;
        while (cursor.moveToNext()) {
            tarea = new Tarea();
            tarea.setId(cursor.getInt(0));
            tarea.setIdUsuarioFk(cursor.getInt(1));
            tarea.setNombreTarea(cursor.getString(2));
            tarea.setDescripcion(cursor.getString(3));
            tarea.setFecha(cursor.getString(4));
            tarea.setCoste(Integer.parseInt(cursor.getString(5)));
            tarea.setPrioridad(cursor.getString(6));
            tarea.setRealizado(Integer.parseInt(cursor.getString(7)));

            ret.add(tarea);
        }
        cursor.close();
        sQLiteDatabase.close();
        return ret;
    }

    //------------------------------ SelectUsuario by Id ------------------------------//

    public Usuario selectUsuarioById (int id) {
        String query = "Select * FROM " + TABLE_NAME_USUARIO + " WHERE " + ID_USUARIO +
                " = " + "'" + id + "'";
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sQLiteDatabase.rawQuery(query, null);

        Usuario usuario;
        if (cursor.moveToFirst()) {
            cursor.moveToFirst();
            usuario = new Usuario();
            usuario.setId(cursor.getInt(0));
            usuario.setNombreUsuario(cursor.getString(1));
            usuario.setPassword(cursor.getString(2));
            cursor.close();
        } else {
            usuario = null;
        }
        sQLiteDatabase.close();
        return usuario;
    }

    //------------------------------ SelectUsuario by Nombre y Password ------------------------------//

    public Usuario selectUsuarioByNombreAndPassword (String nombre, String pass) {
        String query = "Select * FROM " + TABLE_NAME_USUARIO + " WHERE " + NOMBRE +
                " like " + "'" + nombre + "'" + " and " + PASSWORD + " like " + "'" + pass + "'";
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sQLiteDatabase.rawQuery(query, null);

        Usuario usuario;
        if (cursor.moveToFirst()) {
            cursor.moveToFirst();
            usuario = new Usuario();
            usuario.setId(cursor.getInt(0));
            usuario.setNombreUsuario(cursor.getString(1));
            usuario.setPassword(cursor.getString(2));
            cursor.close();
        } else {
            usuario = null;
        }
        sQLiteDatabase.close();
        return usuario;
    }

    //------------------------------ SelectTarea by Id & IdUsuario------------------------------//

    public Tarea selectTareaByIdAndIdUsuario (int id, int idUsuario) {
        String query = "Select * FROM " + TABLE_NAME_TAREA + " WHERE " + ID_TAREA +
                " = " + "'" + id + "'" + " and " + ID_USUARIO_FK + " = " + "'" +
                idUsuario + "'";
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sQLiteDatabase.rawQuery(query, null);

        Tarea tarea;
        if (cursor.moveToFirst()) {
            cursor.moveToFirst();
            tarea = new Tarea();
            tarea.setId(cursor.getInt(0));
            tarea.setIdUsuarioFk(cursor.getInt(1));
            tarea.setNombreTarea(cursor.getString(2));
            tarea.setDescripcion(cursor.getString(3));
            tarea.setFecha(cursor.getString(4));
            tarea.setCoste(Integer.parseInt(cursor.getString(5)));
            tarea.setPrioridad(cursor.getString(6));
            tarea.setRealizado(Integer.parseInt(cursor.getString(7)));
            cursor.close();
        } else {
            tarea = null;
        }
        sQLiteDatabase.close();
        return tarea;
    }

    //------------------------------ InsertUsuario ------------------------------//

    public void insertUsuario (Usuario usuario) {
        ContentValues values = new ContentValues();
        values.put(NOMBRE, usuario.getNombreUsuario());
        values.put(PASSWORD, usuario.getPassword());

        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        sQLiteDatabase.insert(TABLE_NAME_USUARIO, null, values);
        sQLiteDatabase.close();
    }

    //------------------------------ InsertTarea ------------------------------//

    public void insertTarea (Tarea tarea) {
        ContentValues values = new ContentValues();
        values.put(ID_USUARIO_FK, tarea.getIdUsuarioFk());
        values.put(NOMBRE_TAREA, tarea.getNombreTarea());
        values.put(DESCRIPCION, tarea.getDescripcion());
        values.put(FECHA, tarea.getFecha());
        values.put(COSTE, tarea.getCoste());
        values.put(PRIORIDAD, tarea.getPrioridad());
        values.put(REALIZADO, tarea.getRealizado());

        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        sQLiteDatabase.insert(TABLE_NAME_TAREA, null, values);
        sQLiteDatabase.close();
    }

    //------------------------------ UpdateUsuario ------------------------------//

    public boolean updateUsuario (Usuario usuario) {
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        ContentValues args = new ContentValues();
        args.put(NOMBRE, usuario.getNombreUsuario());
        args.put(PASSWORD, usuario.getPassword());
        String whereClause = ID_USUARIO + "=" + usuario.getId();

        return sQLiteDatabase.update(TABLE_NAME_USUARIO, args, whereClause, null) > 0;
    }

    //------------------------------ UpdateTarea ------------------------------//
    public boolean updateTarea (Tarea tarea) {
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        ContentValues args = new ContentValues();
        args.put(NOMBRE_TAREA, tarea.getNombreTarea());
        args.put(DESCRIPCION, tarea.getDescripcion());
        args.put(FECHA, tarea.getFecha());
        args.put(COSTE, tarea.getCoste());
        args.put(PRIORIDAD, tarea.getPrioridad());
        args.put(REALIZADO, tarea.getRealizado());
        String whereClause = ID_TAREA + "="  + tarea.getId() + " AND " + ID_USUARIO_FK + "=" +  tarea.getIdUsuarioFk() ;

        return sQLiteDatabase.update(TABLE_NAME_TAREA, args, whereClause, null) > 0;
    }

    public void updateTareaSQL (Tarea tarea) {
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        String sql="UPDATE "+ TABLE_NAME_TAREA
                + " SET " + NOMBRE_TAREA + "='" + tarea.getNombreTarea() + "',"
                + DESCRIPCION + "='" + tarea.getDescripcion() + "',"
                + FECHA + "='" + tarea.getFecha() + "',"
                + COSTE + "='" + tarea.getCoste() + "',"
                + PRIORIDAD + "='" + tarea.getPrioridad() + "',"
                + REALIZADO + "=" + tarea.getRealizado()
                + " WHERE "+ ID_TAREA + "="  + tarea.getId() + " AND " + ID_USUARIO_FK + "=" +  tarea.getIdUsuarioFk();

        sQLiteDatabase.execSQL(sql);
    }

    //------------------------------ DeleteUsuario ------------------------------//

    public int deleteUsuarioById (int id) {
        int ret;
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Usuario usuario = new Usuario ();
        usuario.setId(id);
        ret = sQLiteDatabase.delete(TABLE_NAME_USUARIO, ID_USUARIO + "=?",
                new String[] {
                        String.valueOf(usuario.getId())
                });
        sQLiteDatabase.close();
        return ret;
    }

    //------------------------------ DeleteTarea ------------------------------//

    public int deleteTareaById (int id) {
        int ret;
        SQLiteDatabase sQLiteDatabase = this.getWritableDatabase();
        Tarea tarea = new Tarea();
        tarea.setId(id);
        ret = sQLiteDatabase.delete(TABLE_NAME_TAREA, ID_TAREA + "=?",
                new String[] {
                        String.valueOf(tarea.getId())
                });
        sQLiteDatabase.close();
        return ret;
    }

    //------------------------------ ifExist / ifEmpty ------------------------------//

    public boolean ifTableExists() {
        boolean ret = false;
        Cursor cursor = null;
        try {
            SQLiteDatabase sQLiteDatabase = this.getReadableDatabase();
            String query = "select DISTINCT tbl_name from sqlite_master where tbl_name = '" +
                    TABLE_NAME_USUARIO + "'";
            cursor = sQLiteDatabase.rawQuery( query, null );
            if (cursor != null) {
                if (cursor.getCount() > 0) {
                    ret = true;
                }
            }
        } catch (Exception e) {
            // Nothing to do here...
        } finally{
            try{
                assert cursor != null;
                cursor.close();
            } catch (NullPointerException e) {
                // Nothing to do here...
            }
        }
        return ret;
    }

    public boolean isEmpty(){
        boolean ret = true;
        Cursor cursor = null;
        try {
            SQLiteDatabase sQLiteDatabase = this.getReadableDatabase();
            cursor = sQLiteDatabase.rawQuery( "SELECT count(*) FROM (select 0 from " +
                    TABLE_NAME_USUARIO + " limit 1)", null );
            cursor.moveToFirst();
            int count = cursor.getInt( 0 );
            if (count > 0) {
                ret = false;
            }
        } catch (Exception e) {
            // Nothing to do here...
        }
        finally {
            try {
                assert cursor != null;
                cursor.close();
            } catch (Exception e) {
                // Nothing to do here...
            }
        }
        return ret;
    }


    /*String query = "Select * FROM " + TABLE_NAME_USUARIO + " WHERE " + NOMBRE +
            " like " + "'" + nombre + "'" + " and " + PASSWORD + " like " + "'" +
            apellido1 + "'" + " and " + APELLIDO2 + " like " + "'" + apellido2 + "'";*/

}