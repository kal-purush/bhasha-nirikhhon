package com.proyectotec.kevin.proyectotec;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import java.util.List;

public class AdapterListas extends RecyclerView.Adapter<AdapterListas.ListasViewHolder> {

    private Context miContexto;
    private List<List> listasUsuario;
    private List<String> listasNombre;

    public AdapterListas(Context miContexto, List<List> listasUsuario,List<String> listasNombre) {
        this.miContexto = miContexto;
        this.listasUsuario = listasUsuario;
        this.listasNombre = listasNombre;
    }


    @Override
    public ListasViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(miContexto);
        View view = inflater.inflate(R.layout.costume_row_listas,null);
        ListasViewHolder holder = new ListasViewHolder(view);
        return holder;
    }

    @Override
    public void onBindViewHolder(@NonNull ListasViewHolder holder, int position) {

        String listaNombre = listasNombre.get(position).toString();
        holder.textViewTitulo.setText(listaNombre);
        holder.textViewCantidad.setText(String.valueOf(getItemCount()));

    }

    @Override
    public int getItemCount() {
        return listasUsuario.size();
    }

    class ListasViewHolder extends RecyclerView.ViewHolder{
        TextView textViewTitulo, textViewCantidad;

        public ListasViewHolder(View itemView) {
            super(itemView);

            textViewTitulo = itemView.findViewById(R.id.titulo);
            textViewCantidad = itemView.findViewById(R.id.cantidad);
        }
    }
}