package ru.otus.hw.commands;

import lombok.RequiredArgsConstructor;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import ru.otus.hw.models.Author;
import ru.otus.hw.services.AuthorService;

import java.util.stream.Collectors;

@RequiredArgsConstructor
@ShellComponent
public class AuthorCommands {

    private final AuthorService authorService;

    @ShellMethod(value = "findAll authors", key = "FAA")
    public String findAllAuthors() {
        return authorService.findAll().stream().map(Author::toString)
                .collect(Collectors.joining("," + System.lineSeparator()));
    }

    @ShellMethod(value = "delete author by id", key = "DA")
    public void deleteGenreById(String id) {
        authorService.deleteById(id);
    }
}