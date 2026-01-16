package com.example.musicapp.Fragment;

import android.os.Bundle;
import android.view.*;
import android.widget.EditText;
import android.widget.Toast;

import androidx.appcompat.app.AlertDialog;
import androidx.fragment.app.Fragment;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.musicapp.R;
import com.example.musicapp.adapter.PlaylistAdapter;
import com.example.musicapp.api.PlaylistAPI;
import com.example.musicapp.dto.PlaylistDTO;
import com.example.musicapp.ultis.RetrofitService;

import java.util.ArrayList;
import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class PlaylistFragment extends Fragment implements PlaylistAdapter.OnPlaylistActionListener {

    private RecyclerView recyclerView;
    private PlaylistAdapter adapter;
    private List<PlaylistDTO> playlistList = new ArrayList<>();
    private PlaylistAPI playlistApi;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.fragment_playlist, container, false);

        recyclerView = view.findViewById(R.id.playlistRecyclerView);
        recyclerView.setLayoutManager(new LinearLayoutManager(getContext()));

        // Retrofit đã có TokenInterceptor, không cần truyền token thủ công
        playlistApi = RetrofitService.getInstance(requireContext()).createService(PlaylistAPI.class);

        adapter = new PlaylistAdapter(playlistList, this);
        recyclerView.setAdapter(adapter);

        loadPlaylists();

        view.findViewById(R.id.addPlaylistButton).setOnClickListener(v -> showAddDialog());

        return view;
    }

    private void loadPlaylists() {
        playlistApi.getAllPlaylists().enqueue(new Callback<List<PlaylistDTO>>() {
            @Override
            public void onResponse(Call<List<PlaylistDTO>> call, Response<List<PlaylistDTO>> response) {
                if (response.isSuccessful() && response.body() != null) {
                    playlistList.clear();
                    playlistList.addAll(response.body());
                    adapter.notifyDataSetChanged();
                } else {
                    Toast.makeText(getContext(), "Không thể tải playlist", Toast.LENGTH_SHORT).show();
                }
            }

            @Override
            public void onFailure(Call<List<PlaylistDTO>> call, Throwable t) {
                Toast.makeText(getContext(), "Lỗi kết nối khi tải playlist", Toast.LENGTH_SHORT).show();
            }
        });
    }

    private void showAddDialog() {
        EditText input = new EditText(getContext());
        new AlertDialog.Builder(requireContext())
                .setTitle("Tên playlist mới")
                .setView(input)
                .setPositiveButton("Tạo", (dialog, which) -> {
                    PlaylistDTO dto = new PlaylistDTO();
                    dto.setName(input.getText().toString());
                    dto.setSongIds(new ArrayList<>());

                    playlistApi.createPlaylist(dto).enqueue(new Callback<PlaylistDTO>() {
                        @Override
                        public void onResponse(Call<PlaylistDTO> call, Response<PlaylistDTO> response) {
                            if (response.isSuccessful()) {
                                loadPlaylists();
                            } else {
                                Toast.makeText(getContext(), "Không thể tạo playlist", Toast.LENGTH_SHORT).show();
                            }
                        }

                        @Override
                        public void onFailure(Call<PlaylistDTO> call, Throwable t) {
                            Toast.makeText(getContext(), "Lỗi khi tạo playlist", Toast.LENGTH_SHORT).show();
                        }
                    });
                })
                .setNegativeButton("Hủy", null)
                .show();
    }

    @Override
    public void onRename(PlaylistDTO playlist) {
        EditText input = new EditText(getContext());
        input.setText(playlist.getName());

        new AlertDialog.Builder(requireContext())
                .setTitle("Đổi tên playlist")
                .setView(input)
                .setPositiveButton("Lưu", (dialog, which) -> {
                    playlist.setName(input.getText().toString());

                    playlistApi.updatePlaylist(playlist.getId(), playlist).enqueue(new Callback<PlaylistDTO>() {
                        @Override
                        public void onResponse(Call<PlaylistDTO> call, Response<PlaylistDTO> response) {
                            if (response.isSuccessful()) {
                                loadPlaylists();
                            } else {
                                Toast.makeText(getContext(), "Không thể đổi tên", Toast.LENGTH_SHORT).show();
                            }
                        }

                        @Override
                        public void onFailure(Call<PlaylistDTO> call, Throwable t) {
                            Toast.makeText(getContext(), "Lỗi khi đổi tên playlist", Toast.LENGTH_SHORT).show();
                        }
                    });
                })
                .setNegativeButton("Hủy", null)
                .show();
    }

    @Override
    public void onDelete(PlaylistDTO playlist) {
        new AlertDialog.Builder(requireContext())
                .setTitle("Xóa playlist")
                .setMessage("Bạn có chắc muốn xóa?")
                .setPositiveButton("Xóa", (dialog, which) -> {
                    playlistApi.deletePlaylist(playlist.getId()).enqueue(new Callback<Void>() {
                        @Override
                        public void onResponse(Call<Void> call, Response<Void> response) {
                            if (response.isSuccessful()) {
                                loadPlaylists();
                            } else {
                                Toast.makeText(getContext(), "Không thể xóa playlist", Toast.LENGTH_SHORT).show();
                            }
                        }

                        @Override
                        public void onFailure(Call<Void> call, Throwable t) {
                            Toast.makeText(getContext(), "Lỗi khi xóa playlist", Toast.LENGTH_SHORT).show();
                        }
                    });
                })
                .setNegativeButton("Hủy", null)
                .show();
    }
}