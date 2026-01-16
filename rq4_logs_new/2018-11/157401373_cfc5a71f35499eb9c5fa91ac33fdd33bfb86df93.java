package ru.fonzy.fnotes.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.fonzy.fnotes.domain.Category;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

@Getter @Setter
@NoArgsConstructor
public class NoteDto {

    private String title;

    @NotNull()
    @Size(min = 1, message = "Текст заметки не может быть пустым")
    @Size(max = 100, message = "Текст заметки слишком большой. Максимальный размер 100 символов")
    private String text;

    private String category;

    private String importance;

}