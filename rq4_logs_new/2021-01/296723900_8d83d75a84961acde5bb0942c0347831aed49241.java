package com.cosc436002fitzarr.brm.models.buildingriskassessment.input;

import com.cosc436002fitzarr.brm.models.GetEntityBySiteInput;
import com.cosc436002fitzarr.brm.models.PageInput;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class GetBuildingRiskAssessmentsBySiteInput {
    private PageInput pageInput;
    private GetEntityBySiteInput getEntityBySiteInput;

    @JsonCreator
    public GetBuildingRiskAssessmentsBySiteInput(@JsonProperty PageInput pageInput, @JsonProperty GetEntityBySiteInput getEntityBySiteInput) {
        this.pageInput = pageInput;
        this.getEntityBySiteInput = getEntityBySiteInput;
    }

    public PageInput getPageInput() {
        return pageInput;
    }

    public void setPageInput(PageInput pageInput) {
        this.pageInput = pageInput;
    }

    public GetEntityBySiteInput getGetEntityBySiteInput() {
        return getEntityBySiteInput;
    }

    public void setGetEntityBySiteInput(GetEntityBySiteInput getEntityBySiteInput) {
        this.getEntityBySiteInput = getEntityBySiteInput;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetBuildingRiskAssessmentsBySiteInput that = (GetBuildingRiskAssessmentsBySiteInput) o;
        return getPageInput().equals(that.getPageInput()) && getGetEntityBySiteInput().equals(that.getGetEntityBySiteInput());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPageInput(), getGetEntityBySiteInput());
    }

    @Override
    public String toString() {
        return "GetBuildingRiskAssessmentsBySiteInput{" +
                "pageInput=" + pageInput +
                ", getEntityBySiteInput=" + getEntityBySiteInput +
                '}';
    }
}