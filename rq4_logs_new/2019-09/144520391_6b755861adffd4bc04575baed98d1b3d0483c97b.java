package com.somamission.peanutbutter.domain;

import lombok.Data;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "reserved_words")
@Data
public class ReservedWord implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "reserved_word_id")
    private Integer wordId;

    @Column(name = "reserved_word")
    private String word;

    @Column(name = "is_admin")
    private String isAdmin;

    @Column(name = "is_abusive")
    private String isAbusive;
}