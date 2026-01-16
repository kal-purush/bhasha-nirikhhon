package com.example.musicapp.adapter;

import android.content.Context;
import android.os.Bundle;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.PopupMenu;
import android.widget.ProgressBar;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;
import com.example.musicapp.R;
import com.example.musicapp.activity.SongDetailActivity;
import com.example.musicapp.api.PlaylistAPI;
import com.example.musicapp.api.SongAPI;
import com.example.musicapp.command.Command;
import com.example.musicapp.command.NavigateToActivityCommand;
import com.example.musicapp.dto.SongDTO;
import com.example.musicapp.dto.SongRatingResponseDTO;
import com.example.musicapp.ultis.RetrofitService;
import com.example.musicapp.dto.PlaylistDTO;

import java.util.ArrayList;
import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class SongSearchAdapter extends RecyclerView.Adapter<RecyclerView.ViewHolder> {

    private final List<SongDTO> songList;
    private final OnSongClickListener listener;
    private final Context context;
    private final SongAPI songAPI;
    private PlaylistAPI playlistAPI;
    private boolean isLoading = false;

    private static final int VIEW_TYPE_SONG = 0;
    private static final int VIEW_TYPE_LOADING = 1;

    public SongSearchAdapter(Context context, List<SongDTO> songList, OnSongClickListener listener) {
        this.context = context;
        this.songList = songList;
        this.listener = listener;
        this.songAPI = RetrofitService.getInstance(context).createService(SongAPI.class);
        this.playlistAPI = RetrofitService.getInstance(context).createService(PlaylistAPI.class);
    }

    public interface OnSongClickListener {
        void onSongClick(SongDTO song);
    }

    @Override
    public int getItemViewType(int position) {
        // Nếu vị trí cuối và đang loading thì hiển thị loading
        return (position == songList.size() && isLoading) ? VIEW_TYPE_LOADING : VIEW_TYPE_SONG;
    }

    @NonNull
    @Override
    public RecyclerView.ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        if (viewType == VIEW_TYPE_LOADING) {
            View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.item_loading, parent, false);
            return new LoadingViewHolder(view);
        } else {
            View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.item_song, parent, false);
            return new SongViewHolder(view);
        }
    }

    @Override
    public void onBindViewHolder(@NonNull RecyclerView.ViewHolder holder, int position) {
        if (holder instanceof SongViewHolder) {
            SongDTO song = songList.get(position);
            SongViewHolder songHolder = (SongViewHolder) holder;

            songHolder.titleText.setText(song.getTitle());
            songHolder.artistText.setText(song.getArtist());
            Glide.with(songHolder.itemView.getContext())
                    .load(song.getImageUrl())
                    .placeholder(R.drawable.placeholder)
                    .into(songHolder.coverImage);

            songAPI.getUserRatingForSong(song.getId()).enqueue(new Callback<>() {
                @Override
                public void onResponse(@NonNull Call<SongRatingResponseDTO> call, @NonNull Response<SongRatingResponseDTO> response) {
                    if (response.isSuccessful() && response.body() != null) {
                        String userRating = response.body().getRating();
                        if ("like".equals(userRating)) {
                            songHolder.likeButton.setImageResource(R.drawable.ic_like_filled);
                            songHolder.dislikeButton.setImageResource(R.drawable.ic_dislike_empty);
                        } else if ("dislike".equals(userRating)) {
                            songHolder.likeButton.setImageResource(R.drawable.ic_like_empty);
                            songHolder.dislikeButton.setImageResource(R.drawable.ic_dislike_filled);
                        } else {
                            songHolder.likeButton.setImageResource(R.drawable.ic_like_empty);
                            songHolder.dislikeButton.setImageResource(R.drawable.ic_dislike_empty);
                        }
                    }
                }

                @Override
                public void onFailure(@NonNull Call<SongRatingResponseDTO> call, @NonNull Throwable t) {
                    Log.e("SongSearchAdapter", "getUserRating error", t);
                }
            });

            songHolder.itemView.setOnClickListener(v -> listener.onSongClick(song));

            songHolder.menuButton.setOnClickListener(v -> {
                PopupMenu popup = new PopupMenu(context, songHolder.menuButton);
                popup.inflate(R.menu.song_menu);
                popup.setOnMenuItemClickListener(item -> {
                    int itemId = item.getItemId();
                    if (itemId == R.id.menu_song_detail) {
                        Bundle extras = new Bundle();
                        extras.putSerializable("song", song);
                        Command goToSongDetail = new NavigateToActivityCommand(context, SongDetailActivity.class, extras);
                        goToSongDetail.execute();
                        return true;
                    } else if (itemId == R.id.menu_add_to_playlist) {
                        showPlaylistDialog(song);
                        return true;
                    }
                    return false;
                });
                popup.show();
            });

            songHolder.likeButton.setOnClickListener(v -> {
                songAPI.likeSong(song.getId()).enqueue(new Callback<>() {
                    @Override
                    public void onResponse(@NonNull Call<SongRatingResponseDTO> call, @NonNull Response<SongRatingResponseDTO> response) {
                        if (response.isSuccessful()) {
                            songHolder.likeButton.setImageResource(R.drawable.ic_like_filled);
                            songHolder.dislikeButton.setImageResource(R.drawable.ic_dislike_empty);
                        }
                    }

                    @Override
                    public void onFailure(@NonNull Call<SongRatingResponseDTO> call, @NonNull Throwable t) {
                        Toast.makeText(context, "Failed to like", Toast.LENGTH_SHORT).show();
                    }
                });
            });

            songHolder.dislikeButton.setOnClickListener(v -> {
                songAPI.dislikeSong(song.getId()).enqueue(new Callback<>() {
                    @Override
                    public void onResponse(@NonNull Call<SongRatingResponseDTO> call, @NonNull Response<SongRatingResponseDTO> response) {
                        if (response.isSuccessful()) {
                            songHolder.likeButton.setImageResource(R.drawable.ic_like_empty);
                            songHolder.dislikeButton.setImageResource(R.drawable.ic_dislike_filled);
                        }
                    }

                    @Override
                    public void onFailure(@NonNull Call<SongRatingResponseDTO> call, @NonNull Throwable t) {
                        Toast.makeText(context, "Failed to dislike", Toast.LENGTH_SHORT).show();
                    }
                });
            });
        }
    }

    private void showPlaylistDialog(SongDTO song) {
        playlistAPI.getAllPlaylists().enqueue(new Callback<>() {
            @Override
            public void onResponse(Call<List<PlaylistDTO>> call, Response<List<PlaylistDTO>> response) {
                if (response.isSuccessful() && response.body() != null) {
                    List<PlaylistDTO> allPlaylists = response.body();
                    List<PlaylistDTO> available = new ArrayList<>();
                    for (PlaylistDTO p : allPlaylists) {
                        if (p.getSongIds() == null || !p.getSongIds().contains(song.getId())) {
                            available.add(p);
                        }
                    }

                    if (available.isEmpty()) {
                        Toast.makeText(context, "All playlists already contain this song", Toast.LENGTH_SHORT).show();
                        return;
                    }

                    String[] names = new String[available.size()];
                    for (int i = 0; i < available.size(); i++) {
                        names[i] = available.get(i).getName();
                    }

                    new android.app.AlertDialog.Builder(context)
                            .setTitle("Add to Playlist")
                            .setItems(names, (dialog, which) -> {
                                PlaylistDTO selected = available.get(which);
                                playlistAPI.addSongToPlaylist(selected.getId(), song.getId()).enqueue(new Callback<>() {
                                    @Override
                                    public void onResponse(Call<PlaylistDTO> call, Response<PlaylistDTO> response) {
                                        Toast.makeText(context, "Added to " + selected.getName(), Toast.LENGTH_SHORT).show();
                                    }

                                    @Override
                                    public void onFailure(Call<PlaylistDTO> call, Throwable t) {
                                        Toast.makeText(context, "Failed to add to playlist", Toast.LENGTH_SHORT).show();
                                    }
                                });
                            }).show();
                }
            }

            @Override
            public void onFailure(Call<List<PlaylistDTO>> call, Throwable t) {
                Toast.makeText(context, "Failed to fetch playlists", Toast.LENGTH_SHORT).show();
            }
        });
    }

    @Override
    public int getItemCount() {
        // Thêm item loading nếu đang ở trạng thái loading
        return songList.size() + (isLoading ? 1 : 0);
    }

    public void showLoading(boolean show) {
        if (this.isLoading != show) {
            this.isLoading = show;
            if (show) {
                // Thêm item loading ở cuối
                notifyItemInserted(songList.size());
            } else {
                // Gỡ bỏ item loading
                notifyItemRemoved(songList.size());
            }
        }
    }

    public static class SongViewHolder extends RecyclerView.ViewHolder {
        TextView titleText, artistText;
        ImageView coverImage, menuButton;
        ImageView likeButton, dislikeButton;

        public SongViewHolder(@NonNull View itemView) {
            super(itemView);
            titleText = itemView.findViewById(R.id.songTitle);
            artistText = itemView.findViewById(R.id.songArtist);
            coverImage = itemView.findViewById(R.id.songCoverImage);
            menuButton = itemView.findViewById(R.id.menu_button);
            likeButton = itemView.findViewById(R.id.like_button);
            dislikeButton = itemView.findViewById(R.id.dislike_button);
        }
    }

    public static class LoadingViewHolder extends RecyclerView.ViewHolder {
        ProgressBar progressBar;

        public LoadingViewHolder(@NonNull View itemView) {
            super(itemView);
            progressBar = itemView.findViewById(R.id.progress_bar);
        }
    }
}