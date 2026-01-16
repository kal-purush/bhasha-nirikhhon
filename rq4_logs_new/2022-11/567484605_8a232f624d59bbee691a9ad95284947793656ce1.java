package com.freddy.ToDoList.models;


import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import java.sql.Date;

@Entity
@Table(name = "todo_item")
public class TodoItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Getter
    @Setter
    @NotBlank(message = "la descripcion no puede estar vacia")
    private String description;

    @Getter
    @Setter
    private Boolean completed;

    @Getter
    @Setter
    private Date createdDate;

    @Getter
    @Setter
    private Date modifiedDate;
}