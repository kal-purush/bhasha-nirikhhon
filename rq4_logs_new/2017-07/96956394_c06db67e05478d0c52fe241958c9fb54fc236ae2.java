package com.pokedex.koto.pokedex;

import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;
import java.util.List;

/**
 * Created by KOTO on 13/07/2017.
 */

public class Adaptador extends RecyclerView.Adapter<Adaptador.PokemonViewHolder> {

    public Adaptador(List<PokemonItem> listPokemon) {
        this.listPokemon = listPokemon;
    }

    List<PokemonItem> listPokemon;
    @Override
    public PokemonViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
     View v = LayoutInflater.from(parent.getContext()).inflate(R.layout.pokemon_item,parent,false);
      PokemonViewHolder holder = new PokemonViewHolder(v);
        return holder;
    }

    @Override
    public void onBindViewHolder(PokemonViewHolder holder, int position) {
        holder.txtName.setText(listPokemon.get(position).getName());
    }

    @Override
    public int getItemCount() {
        return 0;
    }

    public static class PokemonViewHolder extends RecyclerView.ViewHolder{

        TextView txtName;

        public PokemonViewHolder(View itemView){
            super(itemView);
            txtName = (TextView) itemView.findViewById(R.id.txtName);

        }
    }

}