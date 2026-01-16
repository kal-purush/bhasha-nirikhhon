package com.sun.music_64.screen.home;

import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.bumptech.glide.Glide;
import com.sun.music_64.R;
import com.sun.music_64.data.model.Track;
import com.sun.music_64.screen.genrescreen.ItemClickRecyclerView;

import java.util.List;

public class SuggestionAdapter extends RecyclerView.Adapter<SuggestionAdapter.ViewHolder> {

    private List<Track> mtracks;
    private ItemClickRecyclerView mItemClickRecyclerView;

    public SuggestionAdapter(List<Track> tracks, ItemClickRecyclerView itemClickRecyclerView) {
        mtracks = tracks;
        mItemClickRecyclerView = itemClickRecyclerView;
    }

    @NonNull
    @Override
    public SuggestionAdapter.ViewHolder onCreateViewHolder(@NonNull ViewGroup viewGroup, int i) {
        View view = LayoutInflater.from(viewGroup.getContext()).inflate(R.layout.suggest_item_list, viewGroup, false);
        SuggestionAdapter.ViewHolder myViewHolder = new SuggestionAdapter.ViewHolder(view);
        return myViewHolder;
    }

    @Override
    public void onBindViewHolder(@NonNull SuggestionAdapter.ViewHolder viewHolder, int i) {
        viewHolder.setOnItenClickListener(mItemClickRecyclerView);
        viewHolder.bindData(mtracks.get(i));
    }

    @Override
    public int getItemCount() {
        return mtracks == null ? 0 : mtracks.size();
    }

    public void setData(List<Track> tracks) {
        mtracks.clear();
        mtracks.addAll(tracks);
        notifyDataSetChanged();
    }

    static class ViewHolder extends RecyclerView.ViewHolder implements View.OnClickListener {

        private TextView mTextSongName;
        private TextView mTextAuthName;
        private ImageView mImageArtUser;
        private ItemClickRecyclerView mItemClickRecyclerView;

        public ViewHolder(@NonNull View itemView) {
            super(itemView);
            mTextSongName = itemView.findViewById(R.id.text_suggest_song_name);
            mTextAuthName = itemView.findViewById(R.id.text_suggest_auth_name);
            mImageArtUser = itemView.findViewById(R.id.image_suggest_art_user);
            itemView.setOnClickListener(this);
        }

        public void bindData(Track track) {
            mTextSongName.setText(track.getTitle());
            mTextAuthName.setText(track.getArtist());
            Glide.with(itemView)
                    .load(track.getArtworkUrl())
                    .error(R.drawable.ic_error_black_24dp)
                    .into(mImageArtUser);
        }

        void setOnItenClickListener(ItemClickRecyclerView itemClickListener) {
            this.mItemClickRecyclerView = itemClickListener;
        }

        @Override
        public void onClick(View v) {
            mItemClickRecyclerView.onClick(itemView, getLayoutPosition());
        }
    }
}