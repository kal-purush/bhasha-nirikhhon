package ru.otus.spring.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;


@Entity
@Table(name = "AUTHOR")
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Author {
    @Id
    @Column(name = "AUTHOR_ID")
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ_AUTHOR")
    @SequenceGenerator(name = "SEQ_AUTHOR", allocationSize = 1)
    private long id;


    @Column(name = "FULL_NAME", unique = true, nullable = false)
    private String fullName;

}