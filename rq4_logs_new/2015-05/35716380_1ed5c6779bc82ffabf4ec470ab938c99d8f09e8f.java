package dev.kyuelin.hibernate;

import org.hibernate.annotations.Cascade;
import org.hibernate.annotations.CascadeType;

import javax.persistence.*;
import java.util.List;

/**
 * Created by linken on 5/7/15.
 */
@Entity
@Table(name = "family")
public class Family {
    @Id
    @Column(name = "FAMILY_ID")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;

    @OneToMany
    @JoinColumn(name = "FAMILY_ID")
    @Cascade(CascadeType.ALL)
    private List<User> users = null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "FAMILY_NAME")
    private String name;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<User> getUsers() {
        return users;
    }

    public void setUsers(List<User> users) {
        this.users = users;
    }

    public Family() {
    }

    public Family(String name_) {
        this.name=name_;
    }

}