package com.fitastic.entity;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document (collection = "userSessions")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserSession {

    @Id
    private String id;

    @NotBlank
    @Size(min = 5, max = 30)
    private String name;
    @NotBlank
    private String userId;
    @NotBlank
    private String userSessionDateId;

}