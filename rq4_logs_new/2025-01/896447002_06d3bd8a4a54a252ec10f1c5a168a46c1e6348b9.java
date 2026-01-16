package com.example.musing.genre.entity;

import com.example.musing.music.entity.Music;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "genre_music")
@Entity
public class Genre_Music {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "musicid", nullable = false)
    private Music music;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "genreid", nullable = false)
    private Genre genre;


    public static Genre_Music of(Music music, Genre genre) {
        return Genre_Music.builder()
                .music(music)
                .genre(genre)
                .build();
    }
}