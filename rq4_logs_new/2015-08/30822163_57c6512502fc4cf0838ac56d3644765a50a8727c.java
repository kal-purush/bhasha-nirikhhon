package motocitizen.database;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by U_60A9 on 24.08.2015.
 */
public class DbOpenHelper extends SQLiteOpenHelper {

    private static final int    VERSION  = 1;
    private static final String DATABASE = "motodtp";

    public DbOpenHelper(Context context) {
        super(context, DATABASE, null, VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        String createTable = "CREATE TABLE IF NOT EXISTS messages (acc_id int, msg_id int)";
        db.execSQL(createTable);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }
}