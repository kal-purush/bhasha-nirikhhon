package com.ags.dilip.elementcollection;

import java.util.Collection;
import java.util.Set;

import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Employee {
    @Id
    private int id;
    @ElementCollection
    private Collection<VacationEntry> vacationBookings;
    
    @ElementCollection
    private Set<String> name;

    public Collection<VacationEntry> getVacationBookings() {
        return vacationBookings;
    }

    public void setVacationBookings(Collection<VacationEntry> vacationBookings) {
        this.vacationBookings = vacationBookings;
    }

    public Set<String> getName() {
        return name;
    }

    public void setName(Set<String> name) {
        this.name = name;
    }
    
    
}