package com.code.saher.moviesapp.Adapter;

import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.code.saher.moviesapp.R;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;

/**
 * Created by saher on 9/4/2016.
 */
public class TrailerAdapter extends RecyclerView.Adapter<TrailerAdapter.MovieViewHolder> {

    final static String YOUTUBE_API_URL="http://www.youtube.com/watch?v=";

    private ArrayList<com.code.saher.moviesapp.ModelForTrailer.Result> data;
//    private onItemClickListenercustom listener;
    private int rowLayout;
    Context context;
    public TrailerAdapter(Context context, ArrayList<com.code.saher.moviesapp.ModelForTrailer.Result> data, int rowLayout) {
        this.context = context;
        this.data = data;
        this.rowLayout = rowLayout;
//        this.listener=listener;
    }

    @Override
    public MovieViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(rowLayout, parent, false);
        return new MovieViewHolder(view);
    }

    @Override
    public void onBindViewHolder(final MovieViewHolder holder, final int position) {
//        holder.bind(data.get(position), listener);
        holder.trailerTitle.setText(data.get(position).getName());
        Picasso.with(context).load(YOUTUBE_API_URL+data.get(position).getKey())
                .into(holder.youTubeThump);
        holder.playIcon.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                context.startActivity(new Intent(Intent.ACTION_VIEW,
                        Uri.parse(YOUTUBE_API_URL+data.get(position).getKey())));
            }
        });
    }

    @Override
    public int getItemCount() {
        return data.size();
    }

    public static class MovieViewHolder extends RecyclerView.ViewHolder{
        TextView trailerTitle;
        ImageView playIcon;
        ImageView youTubeThump;

        public MovieViewHolder(View itemView) {
            super(itemView);
            trailerTitle = (TextView) itemView.findViewById(R.id.trailer_name);
            playIcon = (ImageView) itemView.findViewById(R.id.trailer_play);
            youTubeThump = (ImageView) itemView.findViewById(R.id.youtube_thumb);
        }
//        public void bind(final Result result, final onItemClickListenercustom listener) {
//            itemView.setOnClickListener(new View.OnClickListener() {
//                @Override
//                public void onClick(View v) {
//                    listener.onItemClick(result);
//
//                }
//            });
//        }
    }
}