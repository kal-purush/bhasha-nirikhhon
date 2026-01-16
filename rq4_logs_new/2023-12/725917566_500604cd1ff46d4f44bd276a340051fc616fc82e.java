package ru.yandex.practicum.filmorate.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.*;
import java.time.LocalDate;

@Getter
@Setter
@Builder
public class User {

    private int id;

    @NotNull
    @Email(message = "Введён некорректный адрес")
    private String email;

    @NotEmpty(message = "Логин не может быть пустым или содержать пробелы")
    @Pattern(regexp = "^\\S+$")
    private String login;
    private String name;

    @NotNull
    @PastOrPresent(message = "День рождения не должен быть в будущем")
    private LocalDate birthday;

}