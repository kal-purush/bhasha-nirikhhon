package com.example.MusicApp.service.strategy;

import com.example.MusicApp.model.Song;
import com.example.MusicApp.repository.SongRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class SearchByArtist implements SearchStrategy {

    @Autowired
    private SongRepository songRepository;

    @Override
    public Page<Song> search(String keyword, Pageable pageable) {
        return songRepository.findByArtist_StageNameContainingIgnoreCase(keyword, pageable);
    }
}