package nemesis.BD;

import android.os.AsyncTask;
import android.util.Log;


/**
 * Created by inigo on 19/07/2015.
 */
public class Consulta extends AsyncTask<Void, Void, Void> {

    JDBCTemplate con = null;
    String consulta="";
    Cursor cursor=null;
    Cursor cursorTemp=null;
    public Consulta(JDBCTemplate con, String consulta){
        this.con = con;this.consulta=consulta;
    }


    @Override
    protected Void doInBackground(Void... params) {
        try {
            con = JDBCTemplate.getJDBCTemplate();
            this. cursorTemp=con.executeQuery(consulta);



        } catch (Exception e) {
            e.printStackTrace();

        }




        return null;
    }
    protected void onPostExecute(Void... params) {
        cursor=cursorTemp;

    }

}