package com.tekenable.model;

import javax.persistence.*;
import java.util.Set;

/**
 * Created by nbarrett on 25/05/2016.
 * The parent class for therapeuticArea. The relationship is like a prent/child relationship where the parent can have
 * one or more children.
 * Note: When this was originally written we only had TherapeuticArea i.e. one level, now we have 2 levels.
 *
 */
@Entity
public class TherapeuticParent extends BaseEntity {

    private String title; //Name of the Therapeutic parent e.g. Cancer,

    private Set<TherapeuticArea> therapeuticareas; //Link to one or more Therapeutic areas e.g. Lung cancer.

    /**
     *
     */
    public TherapeuticParent(){}

    /**
     *
     * @param title
     */
    public TherapeuticParent(String title) {
        this.title =title;
    }


    @Column(name = "title")
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @OneToMany(mappedBy = "therapeuticparent", cascade = {CascadeType.ALL}, fetch = FetchType.LAZY)
    public Set<TherapeuticArea> getTherapeuticareas() {
        return therapeuticareas;
    }

    public void setTherapeuticareas(Set<TherapeuticArea> therapeuticareas) {
        this.therapeuticareas = therapeuticareas;
    }
}