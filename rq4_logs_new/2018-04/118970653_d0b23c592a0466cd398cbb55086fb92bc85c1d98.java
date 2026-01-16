package com.td.models;

public class WorkTD {

    private WorkEffort workEffort;
    private TDStats technicalDebt;

    /**
     * @return the workEffort
     */
    public WorkEffort getWorkEffort() {
        return workEffort;
    }

    /**
     * @param workEffort the workEffort to set
     */
    public void setWorkEffort(WorkEffort workEffort) {
        this.workEffort = workEffort;
    }

    /**
     * @return the technicalDebt
     */
    public TDStats getTechnicalDebt() {
        return technicalDebt;
    }

    /**
     * @param technicalDebt the technicalDebt to set
     */
    public void setTechnicalDebt(TDStats technicalDebt) {
        this.technicalDebt = technicalDebt;
    }

}