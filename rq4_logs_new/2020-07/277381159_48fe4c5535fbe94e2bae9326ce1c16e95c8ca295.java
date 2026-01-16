package com.it2go.micro.employeesservice.persistence.jpa.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.UUID;

@SuppressWarnings("JpaDataSourceORMInspection")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "USER")
public class UserEntity {

    @Id
    @GeneratedValue
    private Long id;

    @Column(name = "PUBLIC_ID", unique = true, nullable = false)
    private UUID publicId;
}