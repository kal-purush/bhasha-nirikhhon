package com.hyep.movietracker.adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;
import com.hyep.movietracker.R;
import com.hyep.movietracker.models.CastModel;
import com.hyep.movietracker.utils.Utils;

import java.util.List;

public class CastDetailMovieAdapter extends RecyclerView.Adapter<CastDetailMovieAdapter.CastDetailMovieHolder> {

    private final List<CastModel> castList;

    private final Context cont;



    public CastDetailMovieAdapter(List<CastModel> castList, Context cont) {
        this.castList = castList;
        this.cont = cont;
    }

    @NonNull
    @Override
    public CastDetailMovieAdapter.CastDetailMovieHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.card_cast, parent, false);
        return new CastDetailMovieHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull CastDetailMovieAdapter.CastDetailMovieHolder holder, int position) {
        CastModel cast = castList.get(position);
        if (cast == null){
            return;
        }

        Glide.with(cont).load(Utils.BASE_IMG_URL+castList.get(position).getProfilePath()).into(holder.imagePoster);
        holder.tvNameCast.setText(cast.getName());
        holder.tvNameCharacter.setText(cast.getCharacter());
    }

    @Override
    public int getItemCount() {
        if (castList != null){
            return castList.size();
        }
        return 0;
    }


    public static class CastDetailMovieHolder extends RecyclerView.ViewHolder{
        private final ImageView imagePoster;

        private final TextView tvNameCast;

        private final TextView tvNameCharacter;

        public CastDetailMovieHolder(@NonNull View itemView) {
            super(itemView);

            imagePoster = itemView.findViewById(R.id.imagePoster);
            tvNameCast = itemView.findViewById(R.id.tvNameCast);
            tvNameCharacter = itemView.findViewById(R.id.tvNameCharacter);
        }
    }
}