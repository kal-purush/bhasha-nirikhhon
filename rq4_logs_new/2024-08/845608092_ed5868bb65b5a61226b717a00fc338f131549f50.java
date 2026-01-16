package com.project.FamilyTree.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.Date;

@Entity
@Table(name = "Member")
@Getter
@Setter
public class MemberModel {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long Id;

    @Column(name = "Full Name", nullable = false)
    private String fullName;

    @Temporal(TemporalType.DATE)
    private Date dateOfBirth;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    private byte[] image;

    @Column(name = "Description")
    private String description;

    @Column(name = "Relative Id", nullable = false)
    private Long relativeId;

    @Column(name = "Relation", nullable = false)
    private String relation;
}