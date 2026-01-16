package com.example.librarymanagement.models.entities;

import jakarta.persistence.*;
import lombok.Data;

import java.util.Date;
import java.util.List;


@Entity
@Data
public class Book {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String title;

    @Column(nullable = false)
    private String author;

    @Column(nullable = false)
    private String publisher;

    @Column(nullable = false)
    private Date publishedDate;

    @Column(nullable = false)
    private String genre;

    @ManyToOne
    @JoinColumn(name = "inventory_id")
    private Inventory inventory;

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "book")
    private List<Transaction> transactions;
}