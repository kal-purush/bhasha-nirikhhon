package com.proyectotec.kevin.proyectotec;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import java.util.List;

public class AdapterEnviarLista extends RecyclerView.Adapter<AdapterEnviarLista.EnviarListaViewHolder> implements View.OnClickListener {

    private Context miContexto;
    private List<List> listasUsuario;
    private List<String> listasNombre;
    private View.OnClickListener listener;


    public AdapterEnviarLista(Context miContexto, List<List> listasUsuario,List<String> listasNombre) {
        this.miContexto = miContexto;
        this.listasUsuario = listasUsuario;
        this.listasNombre = listasNombre;
    }


    @Override
    public EnviarListaViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(miContexto);
        View view = inflater.inflate(R.layout.costume_row_enviarlistas,null);
        EnviarListaViewHolder holder = new EnviarListaViewHolder(view);
        view.setOnClickListener(this);
        return holder;
    }

    @Override
    public void onBindViewHolder(@NonNull EnviarListaViewHolder holder, int position) {

        String listaNombre = listasNombre.get(position).toString();
        holder.textViewTitulo.setText(listaNombre);
        holder.textViewCantidad.setText(String.valueOf(getItemCount()));
        holder.textViewIcon.setText(R.string.id_correo);
    }

    @Override
    public int getItemCount() {
        return listasUsuario.size();
    }

    class EnviarListaViewHolder extends RecyclerView.ViewHolder{

        TextView textViewTitulo,textViewCantidad,textViewIcon;

        public EnviarListaViewHolder(View itemView) {
            super(itemView);
            textViewTitulo = itemView.findViewById(R.id.titulo);
            textViewCantidad = itemView.findViewById(R.id.cantidad);
            textViewIcon = itemView.findViewById(R.id.compartirLista);
        }
    }

    public void setOnClickListener(View.OnClickListener listener){
        this.listener = listener;
    }

    @Override
    public void onClick(View view) {

        if(listener!= null){
            listener.onClick(view);
        }
    }

}