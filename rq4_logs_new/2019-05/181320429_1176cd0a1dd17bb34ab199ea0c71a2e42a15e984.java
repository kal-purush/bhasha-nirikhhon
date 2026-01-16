package org.plan.managementfacade.model.planModel;

import java.util.List;

public class PlanTree {
    private Integer id;
    private String number;
    private String name;
    private List<PlanTree> children;

    public PlanTree(Integer id, String number, String name) {
        this.id = id;
        this.number = number;
        this.name = name;
        this.children = null;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<PlanTree> getChildren() {
        return children;
    }

    public void setChildren(List<PlanTree> children) {
        this.children = children;
    }

    public void addChildren(PlanTree child) {
        children.add(child);
    }
}