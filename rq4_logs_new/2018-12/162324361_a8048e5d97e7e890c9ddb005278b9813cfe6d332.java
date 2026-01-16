import android.content.Context;
import android.database.sqlite.SQLiteDatabase;

import java.sql.SQLException;

public class DataSourceRebost {

    private SQLiteDatabase database;
    private RebostHelper dbAjuda;

    public DataSourceRebost(Context context) { //CONSTRUCTOR
        dbAjuda = new RebostHelper(context);
    }
    public void open() throws SQLException {
        database = dbAjuda.getWritableDatabase();
    }
    public void close() {
        dbAjuda.close();
    }
    //PAG4 DEL PDF DE PERSISTENCIA
}