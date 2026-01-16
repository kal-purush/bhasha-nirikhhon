package com.example.joyeria.entities;

import com.example.joyeria.commons.constants.DatabaseConstant;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

@Getter
@Setter
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = DatabaseConstant.AUTHORITIES_TABLE)
public class AuthorityEntity {

    @Id
    @Column(name = DatabaseConstant.AUTHORITIES_ID, nullable = false, length = 6)
    private String authorityId;

    @Column(name = DatabaseConstant.AUTHORITIES_NAME, nullable = false, length = 6, unique = true)
    private String authorityName;

}