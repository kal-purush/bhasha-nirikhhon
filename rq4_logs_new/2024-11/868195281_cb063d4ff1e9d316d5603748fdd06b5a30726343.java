package com.filegenie.backend.Entities;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "configuration_files")
public class ConfigurationFile extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long configId;

    @Column(nullable = false)
    private String configName;

    @Column(nullable = false)
    private String configVersion;

    @Column(nullable = false)
    private CONFIG_TYPE configType;

    @OneToMany(mappedBy = "configurationFile", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Field> fields;

    @ManyToMany(mappedBy = "configurationFiles")
    private Set<User> users;

    public enum CONFIG_TYPE {
        XML,
        JSON,
        YAML,
        CSV,
    }
}