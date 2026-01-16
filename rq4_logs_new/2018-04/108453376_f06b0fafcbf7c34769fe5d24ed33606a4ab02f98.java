/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package at.racemanager.api.entity;

import static at.racemanager.api.entity.Driver.COUNT_RESULTS;
import static at.racemanager.api.entity.Driver.FIND_ALL;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.MappedSuperclass;

/**
 *
 * @author rolhai
 */
@MappedSuperclass
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public abstract class ApiEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    protected Long id;

    public Long getId() {
        return id;
    }

    protected void setId(Long id) {
        this.id = id;
    }

    protected static String getCountResultNamedQuery() {
        return COUNT_RESULTS;
    }

    protected static String getFindAllNamedQuery() {
        return FIND_ALL;
    }
}