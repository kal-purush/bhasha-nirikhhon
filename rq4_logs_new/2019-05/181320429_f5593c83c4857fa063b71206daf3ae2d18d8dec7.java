package org.plan.managementfacade.model.planModel.requestModel;

import java.util.List;

public class PlanTreeForGantt {
    private Integer id;
    private String name;
    private String projectType;
    private Integer order;
    private Integer quantity;
    private String startDate;
    private String endDate;
    private String createrName;
    private boolean haveException;
    private List<PlanTreeForGantt> children;

    public PlanTreeForGantt(Integer id, String name, String projectType, Integer order, Integer quantity, String startDate, String endDate, String createrName, boolean haveException) {
        this.id = id;
        this.name = name;
        this.projectType = projectType;
        this.order = order;
        this.quantity = quantity;
        this.startDate = startDate;
        this.endDate = endDate;
        this.createrName = createrName;
        this.haveException = haveException;
        this.children = null;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProjectType() {
        return projectType;
    }

    public void setProjectType(String projectType) {
        this.projectType = projectType;
    }

    public Integer getOrder() {
        return order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getCreaterName() {
        return createrName;
    }

    public void setCreaterName(String createrName) {
        this.createrName = createrName;
    }

    public boolean isHaveException() {
        return haveException;
    }

    public void setHaveException(boolean haveException) {
        this.haveException = haveException;
    }

    public List<PlanTreeForGantt> getChildren() {
        return children;
    }

    public void setChildren(List<PlanTreeForGantt> children) {
        this.children = children;
    }

    public void addChildren (PlanTreeForGantt child) {
        this.children.add(child);
    }
}