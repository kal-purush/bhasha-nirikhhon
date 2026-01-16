package com.xebia.jpaexcercises._12_many_to_many;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "employees")
public class Employee {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private String name;
    private long salary;
    @ManyToMany
    private List<Project> projects;

    public Employee() {
    }

    public Employee(String name, long salary) {
        this.name = name;
        this.salary = salary;
    }

    public Employee addProject(Project project) {
        if (projects == null) {
            projects = new ArrayList<>();
        }
        projects.add(project);
        return this;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getSalary() {
        return salary;
    }

    public List<Project> getProjects() {
        return projects;
    }
}