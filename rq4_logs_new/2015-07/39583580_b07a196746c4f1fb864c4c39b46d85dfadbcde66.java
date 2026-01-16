package nemesis.diiscover;

import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.TextView;

import java.sql.ResultSet;

import nemesis.AsyncResponse;
import nemesis.BD.Consulta;
import nemesis.BD.Cursor;


public class listado_grado extends ActionBarActivity implements AsyncResponse {
    Consulta consultaUsarios=null;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_listado_grado);
        consultaUsarios = new Consulta("Carreras","Select * from Carrera");
        consultaUsarios.delegate = this;
        consultaUsarios.execute();//mirar el metodo processFinish
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_listado_grado, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    public void processFinish(Cursor cursor,String output){
        switch (output) {
            case "Carreras":

                //AQUÍ UTILIZAR consultaUusarios.cursor que ya estará inicializado
                try{
                    ResultSet result= cursor.getResultSet ();
                    while(result.next()){
                        //String usuario=result.getString("nip");
                        /**
                         * id INT AUTO_INCREMENT,
                         nombre VARCHAR(100),
                         coordinador VARCHAR(100),
                         descripcion VARCHAR(1000),
                         linkExterno VARCHAR(150),
                         fotoURL VARCHAR(100),
                         cuatrimestres INT,
                         */
                    }


                }  catch(Exception a){}

                break;
            case "otros":


                break;

            default:


                break;
        }
    }
}