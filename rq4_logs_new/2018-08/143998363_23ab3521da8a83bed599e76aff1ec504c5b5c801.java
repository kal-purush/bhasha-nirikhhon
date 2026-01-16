package com.condescendors.pollingapp.pollingapp.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.condescendors.pollingapp.pollingapp.models.audit.DateAudit;
import org.hibernate.annotations.NaturalId;

import javax.persistence.*;
import javax.validation.constraints.Max;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "user", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"login"}),
        @UniqueConstraint(columnNames = {"email"})
})
public class User extends DateAudit implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id")
    private Long id;

    @NotBlank
    @Max(value = 25)
    @Column(name = "first_name")
    private String firstName;

    @Max(value = 25)
    @NotBlank
    @Column(name = "last_name")
    private String lastName;

    @Max(value = 15)
    @NotBlank
    @Column(name = "login")
    private String userName;

    @NaturalId
    @Max(value = 100)
    @NotBlank
    @Column(name = "email")
    private String email;

    @NotBlank
    @JsonIgnore
    @Column(name = "password")
    private String password;

    @ManyToMany
    @JoinTable(
            name = "user_auhtority",
            joinColumns = @JoinColumn(name = "user_id"),
            inverseJoinColumns = @JoinColumn(name = "name")
    )
    @Column(name = "authorities")
    private Set<Authority> roles=new HashSet<>();

    public User(@NotBlank @Max(value = 25) String firstName, @Max(value = 25) @NotBlank String lastName, @Max(value = 15) @NotBlank String userName, @Max(value = 100) @NotBlank String email, @NotBlank String password, Set<Authority> roles) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.userName = userName;
        this.email = email;
        this.password = password;
        this.roles = roles;
    }

    public User() {
    }


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Set<Authority> getRoles() {
        return roles;
    }

    public void setRoles(Set<Authority> roles) {
        this.roles = roles;
    }
}