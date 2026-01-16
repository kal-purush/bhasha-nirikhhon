package com.alibou.banking.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.TableGenerator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "STUDENTS")
public class Student {

    @Id
    /*@SequenceGenerator(
            name = "std_seq",
            sequenceName = "std_seq",
            allocationSize = 1,
            initialValue = 100
    )
    @GeneratedValue(generator = "std_seq", strategy = GenerationType.SEQUENCE)*/
    @TableGenerator(
            name = "std_tbl",
            allocationSize = 1,
            initialValue = 100
    )
    @GeneratedValue(generator = "std_tbl", strategy = GenerationType.TABLE)
    private Integer id;
    @Column(name = "F_NAME")
    private String firstname;
    @Column(name = "L_NAME")
    private String lastname;
    @Column(unique = true, nullable = false, length = 100)
    private String email;
    private int age;
}