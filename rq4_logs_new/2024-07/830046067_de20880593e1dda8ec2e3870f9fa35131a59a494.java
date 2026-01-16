package com.example.librarymanagement.dto;

import com.example.librarymanagement.model.entity.Author;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class AuthorDto {
    @Size(max = 50, message = "First name must contain no more than 50 characters")
    @Pattern(regexp = "[A-Z][a-z]+",
            message = "First name must start with a capital letter followed by one or more lowercase letters")
    @NotBlank(message = "First name can't be empty")
    private String firstName;

    @Size(max = 50, message = "Last name must contain no more than 50 characters")
    @Pattern(regexp = "[A-Z][a-z]+",
            message = "Last name must start with a capital letter followed by one or more lowercase letters")
    @NotBlank(message = "Last name can't be empty")
    private String lastName;

    @Size(max = 50, message = "Pseudonym must contain no more that 50 characters")
    @Pattern(regexp = "\\w", message = "Pseudonym must contain of characters")
    private String pseudonym;

    public AuthorDto(Author author){
        this.firstName = author.getFirstName();
        this.lastName = author.getLastName();
        this.pseudonym = author.getPseudonym();
    }
}