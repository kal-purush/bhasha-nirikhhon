package kr.pe.lahuman.models;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * Created by lahuman on 2016. 2. 12..
 */
@Entity
@Data
public class Notice extends DefaultEntity {

    @Id
    @GeneratedValue
    private Long id;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String contents;

    @Column(length = 1000, nullable = false)
    private String title;
}