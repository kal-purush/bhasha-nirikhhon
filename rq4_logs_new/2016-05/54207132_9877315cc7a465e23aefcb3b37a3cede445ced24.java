package ceindetec.mesabar.Controller;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.ListView;
import android.widget.RelativeLayout;
import android.widget.SearchView;

import ceindetec.mesabar.R;
import ceindetec.mesabar.Transactions.TransactionArrayAdapterAleatorio;
import ceindetec.mesabar.Transactions.TransactionArrayAdapterListado;

import java.util.HashMap;

public class ControllerActivityBusqueda extends AppCompatActivity {

    //URL's de acceso a los JSON de datos
    private static String URL_JSON_CATEGORIA_ALEATORIO = "getListCategoriaAleatorio";
    private static String URL_JSON_BUSQUEDA_RESULTADO = "getListBusquedaResultado";

    //Declaracion de variables de paso de parametros
    HashMap<String, String> parameters = new HashMap<>();

    //Declaracion de los elementos que se utilizaran en la activity
    SearchView searchView;

    RelativeLayout relativeLayout;

    ListView lvListaBusquedaConsulta;
    ListView lvListaCategoriaAleatoria;

    ArrayAdapter adaptadorArregloBusquedaConsulta;
    ArrayAdapter adaptadorArregloAleatoria;

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_busqueda);

        searchView = (SearchView) findViewById(R.id.scvBusqueda);
        searchView.setFocusable(true);
        searchView.setIconified(false);
        searchView.requestFocusFromTouch();
        searchView.setOnQueryTextListener(new SearchView.OnQueryTextListener() {
            @Override
            public boolean onQueryTextSubmit(String queryText) {

                return true;
            }

            @Override
            public boolean onQueryTextChange(String queryText) {

                if (queryText.length() >= 3) {

                    parameters.put("query", queryText);

                    //Obtener instancia de la lista
                    lvListaBusquedaConsulta = (ListView) findViewById(R.id.lvListaBusquedaConsulta);

                    //Crear adaptador
                    adaptadorArregloBusquedaConsulta = new TransactionArrayAdapterListado(getBaseContext(), URL_JSON_BUSQUEDA_RESULTADO, parameters);

                    //Ingresa el adaptador en la instancia de la lista
                    lvListaBusquedaConsulta.setAdapter(adaptadorArregloBusquedaConsulta);

                    relativeLayout = (RelativeLayout) findViewById(R.id.rlyBarraCercaDeMi);
                    relativeLayout.setVisibility(View.INVISIBLE);
                    relativeLayout = (RelativeLayout) findViewById(R.id.rlyBusquedaCategoriaAleatoria);
                    relativeLayout.setVisibility(View.INVISIBLE);
                    relativeLayout = (RelativeLayout) findViewById(R.id.rlyBusquedaConsulta);
                    relativeLayout.setVisibility(View.VISIBLE);

                    return true;

                } else {

                    relativeLayout = (RelativeLayout) findViewById(R.id.rlyBarraCercaDeMi);
                    relativeLayout.setVisibility(View.VISIBLE);
                    relativeLayout = (RelativeLayout) findViewById(R.id.rlyBusquedaCategoriaAleatoria);
                    relativeLayout.setVisibility(View.VISIBLE);
                    relativeLayout = (RelativeLayout) findViewById(R.id.rlyBusquedaConsulta);
                    relativeLayout.setVisibility(View.INVISIBLE);

                    return false;

                }
            }
        });

        //Obtener instancia de la lista
        lvListaCategoriaAleatoria = (ListView) findViewById(R.id.lvListaCategoriaAleatoria);

        //Crear adaptador
        adaptadorArregloAleatoria = new TransactionArrayAdapterAleatorio(this, URL_JSON_CATEGORIA_ALEATORIO);

        //Ingresa el adaptador en la instancia de la lista
        lvListaCategoriaAleatoria.setAdapter(adaptadorArregloAleatoria);
    }

}