package ru.otus.marchenko.mappers;

import org.springframework.stereotype.Component;
import ru.otus.marchenko.dto.author.AuthorCreateDto;
import ru.otus.marchenko.dto.author.AuthorDto;
import ru.otus.marchenko.models.Author;

@Component
public class AuthorMapperImpl implements AuthorMapper{

    @Override
    public AuthorDto toDto(Author author) {
        return new AuthorDto(author.getId(), author.getFullName());
    }

    @Override
    public Author toModel(AuthorCreateDto authorDto) {
        return new Author(null, authorDto.fullName());
    }

    @Override
    public Author toModel(AuthorDto authorDto) {
        return new Author(authorDto.id(), authorDto.fullName());
    }
}