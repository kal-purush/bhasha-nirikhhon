package com.udacity.jdnd.course3.critter.entity;

import com.udacity.jdnd.course3.critter.user.EmployeeSkill;

import javax.persistence.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Entity
@Table(name="SCHEDULE")
public class Schedule {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name="SCHEDULE_ID")
    private long id;

    @ManyToMany(cascade=CascadeType.ALL, fetch=FetchType.LAZY)
    @JoinTable(name="SCHEDULE_EMPLOYEE",
            joinColumns={@JoinColumn (name="employee_id")},
            inverseJoinColumns={@JoinColumn(name="schedule_id")})
    private List<Employee> employees=new ArrayList<>();

    @ManyToMany(cascade=CascadeType.ALL, fetch=FetchType.LAZY)
    @JoinTable(name="SCHEDULE_PET",
            joinColumns={@JoinColumn (name="pet_id")},
            inverseJoinColumns={@JoinColumn(name="schedule_id")})
    private List<Pet> pets=new ArrayList<>();

    @Column(name="SCHEDULE_DATE")
    private LocalDate date;

    @ElementCollection
    @Column(name="ACTIVITIES")
    private Set<EmployeeSkill> activities=new HashSet<>();

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public List<Employee> getEmployees() {
        return employees;
    }

    public void setEmployees(List<Employee> employees) {
        this.employees = employees;
    }

    public List<Pet> getPets() {
        return pets;
    }

    public void setPets(List<Pet> pets) {
        this.pets = pets;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public Set<EmployeeSkill> getActivities() {
        return activities;
    }

    public void setActivities(Set<EmployeeSkill> activities) {
        this.activities = activities;
    }
}