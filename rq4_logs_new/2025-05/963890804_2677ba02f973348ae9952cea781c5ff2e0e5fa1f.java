package com.example.musicapp.Fragment;

import android.media.AudioManager;
import android.media.MediaPlayer;
import android.os.Bundle;
import android.os.Handler;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageButton;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.musicapp.R;
import com.example.musicapp.adapter.SongAdapter;
import com.example.musicapp.api.SongAPI;
import com.example.musicapp.ultis.RetrofitService;
import com.example.musicapp.api.PlaylistAPI;
import com.example.musicapp.dto.SongDTO;
import com.google.android.material.button.MaterialButton;
import com.google.android.material.slider.Slider;

import java.util.List;
import java.util.Locale;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class PlaylistDetailFragment extends Fragment {

    private static final String TAG = "PLAYER";
    private RecyclerView recyclerView;
    private PlaylistAPI playlistAPI;

    private List<SongDTO> songList;

    private Long playlistId;

    private MediaPlayer mediaPlayer;
    private int currentSongIndex = -1;
    private boolean isPlaying = false;

    private MaterialButton nextBtn, playButton;
    private MaterialButton previousBtn;
    private Slider seekBar;
    private TextView songTitleTextView;
    private TextView songArtistTextView;
    private TextView startTimeTextView, endTimeTextView;
    private Handler handler = new Handler();

    public static PlaylistDetailFragment newInstance(Long playlistId) {
        PlaylistDetailFragment fragment = new PlaylistDetailFragment();
        Bundle args = new Bundle();
        args.putLong("playlistId", playlistId);
        fragment.setArguments(args);
        return fragment;
    }

    @Override
    public void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        if (getArguments() != null) {
            playlistId = getArguments().getLong("playlistId");
        }
        RetrofitService retrofitService = RetrofitService.getInstance(requireContext());
        playlistAPI = retrofitService.createService(PlaylistAPI.class);
    }

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater,
                             @Nullable ViewGroup container,
                             @Nullable Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.fragment_playlist_detail, container, false);
        recyclerView = view.findViewById(R.id.recyclerViewSongsInPlaylist);
        recyclerView.setLayoutManager(new LinearLayoutManager(getContext()));
        ImageButton backButton = view.findViewById(R.id.backButton);
        playButton = view.findViewById(R.id.play_button);
        nextBtn = view.findViewById(R.id.next_button);
        previousBtn = view.findViewById(R.id.prev_button);
        seekBar = view.findViewById(R.id.song_progress_slider);
        startTimeTextView = view.findViewById(R.id.current_time);
        endTimeTextView = view.findViewById(R.id.total_time);
        songTitleTextView = view.findViewById(R.id.songTitleTextView);
        songArtistTextView = view.findViewById(R.id.songArtistTextView);

        backButton.setOnClickListener(v -> {
            requireActivity().getSupportFragmentManager().popBackStack();
        });

        loadSongsInPlaylist();

        return view;
    }

    private void loadSongsInPlaylist() {
        Call<List<SongDTO>> call = playlistAPI.getSongsInPlaylist(playlistId);
        call.enqueue(new Callback<List<SongDTO>>() {
            @Override
            public void onResponse(Call<List<SongDTO>> call, Response<List<SongDTO>> response) {
                if (response.isSuccessful() && response.body() != null) {
                    songList = response.body();
                    SongAdapter adapter = new SongAdapter(requireContext(), songList, song -> {
                        if (isPlaying) {
                            stopMusic();
                        }
                        currentSongIndex = songList.indexOf(song);
                        updateSongInfo(song);
                        Log.d(TAG, "Selected song: " + song.getTitle() + " by " + song.getArtist());
                    });
                    recyclerView.setAdapter(adapter);
                } else {
                    Toast.makeText(getContext(), "Failed to fetch songs!", Toast.LENGTH_SHORT).show();
                }
            }

            @Override
            public void onFailure(Call<List<SongDTO>> call, Throwable t) {
                Toast.makeText(getContext(), "Error loading songs", Toast.LENGTH_SHORT).show();
            }
        });

        playButton.setOnClickListener(v -> {
            if (currentSongIndex == -1 || songList == null || songList.isEmpty()) {
                Toast.makeText(getContext(), "Vui lòng chọn bài hát!", Toast.LENGTH_SHORT).show();
                return;
            }

            if (mediaPlayer == null) {
                startMusic(songList.get(currentSongIndex).getFileUrl());
            } else if (mediaPlayer.isPlaying()) {
                mediaPlayer.pause();
                isPlaying = false;
                playButton.setIconResource(R.drawable.ic_play);
                Log.d(TAG, "Paused");
            } else {
                mediaPlayer.start();
                isPlaying = true;
                playButton.setIconResource(R.drawable.ic_pause);
                startUpdatingSeekBar();
            }
        });

        nextBtn.setOnClickListener(v -> {
            if (songList == null || songList.isEmpty()) return;

            if (isPlaying) {
                stopMusic();
            }
            currentSongIndex = (currentSongIndex + 1) % songList.size();
            updateSongInfo(songList.get(currentSongIndex));
            startMusic(songList.get(currentSongIndex).getFileUrl());
        });

        previousBtn.setOnClickListener(v -> {
            if (songList == null || songList.isEmpty()) return;

            if (isPlaying) {
                stopMusic();
            }
            currentSongIndex = (currentSongIndex - 1 + songList.size()) % songList.size();
            updateSongInfo(songList.get(currentSongIndex));
            startMusic(songList.get(currentSongIndex).getFileUrl());
        });

        seekBar.addOnChangeListener((slider, value, fromUser) -> {
            if (fromUser && mediaPlayer != null) {
                int seekPosition = (int) (mediaPlayer.getDuration() * (value / 100));
                mediaPlayer.seekTo(seekPosition);
            }
        });
    }

    private void updateSongInfo(SongDTO song) {
        songTitleTextView.setText(song.getTitle());
        songArtistTextView.setText(song.getArtist());
    }

    private void startMusic(String url) {
        releasePlayer();

        mediaPlayer = new MediaPlayer();
        mediaPlayer.setAudioStreamType(AudioManager.STREAM_MUSIC);

        try {
            mediaPlayer.setDataSource(url);
            mediaPlayer.setOnPreparedListener(mp -> {
                mp.start();
                isPlaying = true;
                playButton.setIconResource(R.drawable.ic_pause);

                seekBar.setValue(0);
                seekBar.setValueFrom(0);
                seekBar.setValueTo(100);

                int totalDuration = mp.getDuration();
                endTimeTextView.setText(formatTime(totalDuration));

                startUpdatingSeekBar();
                Log.d(TAG, "Playing: " + url);

                // Gọi API tăng view
                if (songList != null && currentSongIndex >= 0) {
                    long songId = songList.get(currentSongIndex).getId();
                    SongAPI songAPI = RetrofitService.getInstance(requireContext()).createService(SongAPI.class);
                    songAPI.incrementView(songId).enqueue(new Callback<Void>() {
                        @Override
                        public void onResponse(Call<Void> call, Response<Void> response) {
                            if (response.isSuccessful()) {
                                Log.d(TAG, "View count updated for song id: " + songId);
                            } else {
                                Log.w(TAG, "Failed to update view. Code: " + response.code());
                            }
                        }

                        @Override
                        public void onFailure(Call<Void> call, Throwable t) {
                            Log.e(TAG, "Error updating view: " + t.getMessage());
                        }
                    });
                }
            });

            mediaPlayer.setOnCompletionListener(mp -> {
                isPlaying = false;
                playButton.setIconResource(R.drawable.ic_play);
                handler.removeCallbacksAndMessages(null);
                seekBar.setValue(0);
                startTimeTextView.setText(formatTime(0));
                releasePlayer();
            });

            mediaPlayer.prepareAsync();
        } catch (Exception e) {
            Log.e(TAG, "Exception: " + e.getMessage());
        }
    }

    private void startUpdatingSeekBar() {
        handler.postDelayed(new Runnable() {
            @Override
            public void run() {
                if (mediaPlayer != null && isPlaying) {
                    int currentPosition = mediaPlayer.getCurrentPosition();
                    int duration = mediaPlayer.getDuration();

                    if (duration > 0) {
                        float progress = (currentPosition * 100f) / duration;
                        seekBar.setValue(Math.min(progress, 100f));
                        startTimeTextView.setText(formatTime(currentPosition));
                    }
                    handler.postDelayed(this, 500);
                }
            }
        }, 0);
    }

    private String formatTime(int millis) {
        int minutes = (millis / 1000) / 60;
        int seconds = (millis / 1000) % 60;
        return String.format(Locale.getDefault(), "%02d:%02d", minutes, seconds);
    }

    private void stopMusic() {
        if (mediaPlayer != null && mediaPlayer.isPlaying()) {
            mediaPlayer.stop();
            mediaPlayer.reset();
            mediaPlayer.release();
            mediaPlayer = null;
            isPlaying = false;
            playButton.setIconResource(R.drawable.ic_play);
            handler.removeCallbacksAndMessages(null);
        }
    }

    private void releasePlayer() {
        if (mediaPlayer != null) {
            if (mediaPlayer.isPlaying()) {
                mediaPlayer.stop();
            }
            mediaPlayer.reset();
            mediaPlayer.release();
            mediaPlayer = null;
            isPlaying = false;
            playButton.setIconResource(R.drawable.ic_play);
        }
    }

    @Override
    public void onDestroyView() {
        super.onDestroyView();
        releasePlayer();
        handler.removeCallbacksAndMessages(null);
    }

}