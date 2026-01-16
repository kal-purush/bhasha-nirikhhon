package org.plan.managementfacade.model.planModel;

import java.util.List;

public class DistributePlanReq {
    private Integer planId;
    private List<Integer> executerIdList;

    public Integer getPlanId() {
        return planId;
    }

    public void setPlanId(Integer planId) {
        this.planId = planId;
    }

    public List<Integer> getExecuterIdList() {
        return executerIdList;
    }

    public void setExecuterIdList(List<Integer> executerIdList) {
        this.executerIdList = executerIdList;
    }
}