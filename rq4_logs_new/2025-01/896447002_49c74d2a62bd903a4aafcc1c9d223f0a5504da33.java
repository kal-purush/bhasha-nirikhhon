package com.example.musing.genre.dto;

import com.example.musing.genre.entity.Genre;
import com.example.musing.genre.entity.Genre_Music;
import lombok.Builder;

@Builder
public record GenreDto(
        long id,
        String genreName
) {
    public static GenreDto toDto(Genre_Music genreMusic){
        return GenreDto.builder()
                .id(genreMusic.getId())
                .genreName(genreMusic.getGenre().getGenreName().getKey())
                .build();
    }
    public static GenreDto toDto(Genre genre){
        return GenreDto.builder()
                .id(genre.getId())
                .genreName(genre.getGenreName().getKey())
                .build();
    }
}