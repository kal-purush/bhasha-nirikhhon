package ru.otus.marchenko.models.dto.book;
import ru.otus.marchenko.models.dto.author.AuthorDto;
import ru.otus.marchenko.models.dto.genre.GenreDto;

public record BookDto(
        Long id,
        String title,
        AuthorDto authorDto,
        GenreDto genreDto) {
}