package com.example.agenda4;

import android.content.Context;
import android.database.DataSetObserver;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ListAdapter;
import android.widget.TextView;

import java.util.ArrayList;

/**
 * Created by luis on 21/11/2017.
 */

public class Adaptador implements ListAdapter{
    //ListAdapter:Extended Adapter that is the bridge between a ListView and the data that backs the list.
    //Frequently that data comes from a Cursor, but that is not required.
    // //The ListView can display any data provided that it is wrapped in a ListAdapter.
    ArrayList<Contacto> lista_contactos;
    Context contexto;

    public Adaptador(ArrayList<Contacto> lista_contactos, Context contexto) {
        this.lista_contactos = lista_contactos;
        this.contexto = contexto;
    }

    @Override
    public void registerDataSetObserver(DataSetObserver dataSetObserver) {

    }

    @Override
    public void unregisterDataSetObserver(DataSetObserver dataSetObserver) {

    }

    @Override
    public int getCount() {
        return lista_contactos.size();
    }

    @Override
    public Object getItem(int i) {
        //Get the data item associated with the specified position in the data set.
        return lista_contactos.get(i);
    }

    @Override
    public long getItemId(int i) {
        //Get the row id associated with the specified position in the list.
        return i;
    }

    @Override
    public boolean hasStableIds() {
        return false;
    }

    @Override
    public View getView(int i, View view, ViewGroup viewGroup) {
        LayoutInflater inflater=LayoutInflater.from(contexto);
        View vista=inflater.inflate(R.layout.contacto, viewGroup, false);
        TextView tv_nombre_lv=(TextView)vista.findViewById(R.id.tv_nombre_lv);
        TextView tv_apellido_lv=(TextView)vista.findViewById(R.id.tv_apellido_lv);
        TextView tv_email_lv=(TextView)vista.findViewById(R.id.tv_email_lv);
        tv_nombre_lv.setText(lista_contactos.get(i).getNombre());
        tv_apellido_lv.setText(lista_contactos.get(i).getApellido());
        tv_email_lv.setText(lista_contactos.get(i).getEmail());
        return vista;
    }

    @Override
    public int getItemViewType(int i) {
        Log.v("getItemViewType", String.valueOf(i));
        return lista_contactos.size()-1;
    }

    @Override

    public int getViewTypeCount() {
        return lista_contactos.size();
    }

    @Override
    public boolean isEmpty() {
        return lista_contactos.isEmpty();
    }

    @Override
    public boolean areAllItemsEnabled() {
        //Indicates whether all the items in this adapter are enabled.
        // If the value returned by this method changes over time, there is no guarantee it will take effect.
        // If true, it means all items are selectable and clickable (there is no separator.)
        return true;
    }

    @Override
    public boolean isEnabled(int i) {
        //Returns true if the item at the specified position is not a separator.

        return true;
    }
}