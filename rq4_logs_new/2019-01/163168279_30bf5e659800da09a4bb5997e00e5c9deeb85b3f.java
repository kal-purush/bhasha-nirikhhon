package ca.clubFinder.user;

import ca.clubFinder.club.Club;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.CascadeType;
import javax.persistence.JoinColumn;
import javax.persistence.FetchType;
import javax.persistence.OneToMany;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class User {
    @Id
    @GeneratedValue
    private long id;
    private String email;
    private String name;
    private String phoneNumber;

    //@OneToMany(fetch=FetchType.LAZY, cascade=CascadeType.ALL, mappedBy="user")
    //private Set<Club> clubs;
}