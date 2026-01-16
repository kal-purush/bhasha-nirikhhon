package com.upc.model.dto;

import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * Created by Qloop on 2017/5/21.
 */
public class StuReportDto {

    private String result;
    private long id;

    private String goalRept;
    private String theoryRept;
    private String equipmentRept;
    private String demandRept;
    private String warningRept;
    private String imgs;// 上传的图片url  (';'拼接)

    private long templateId;
    private long stuId;
    private String stuName;
    private String stuClass;
    private String dataRept;
    private String thinkingRept;
    private Date createDate;

    public void setResult(String result) {
        this.result = result;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setGoalRept(String goalRept) {
        this.goalRept = goalRept;
    }

    public void setTheoryRept(String theoryRept) {
        this.theoryRept = theoryRept;
    }

    public void setEquipmentRept(String equipmentRept) {
        this.equipmentRept = equipmentRept;
    }

    public void setDemandRept(String demandRept) {
        this.demandRept = demandRept;
    }

    public void setWarningRept(String warningRept) {
        this.warningRept = warningRept;
    }

    public void setImgs(String imgs) {
        this.imgs = imgs;
    }

    public void setTemplateId(long templateId) {
        this.templateId = templateId;
    }

    public void setStuId(long stuId) {
        this.stuId = stuId;
    }

    public void setStuName(String stuName) {
        this.stuName = stuName;
    }

    public void setStuClass(String stuClass) {
        this.stuClass = stuClass;
    }

    public void setDataRept(String dataRept) {
        this.dataRept = dataRept;
    }

    public void setThinkingRept(String thinkingRept) {
        this.thinkingRept = thinkingRept;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public String getResult() {
        return result;
    }

    public long getId() {
        return id;
    }

    public String getGoalRept() {
        return goalRept;
    }

    public String getTheoryRept() {
        return theoryRept;
    }

    public String getEquipmentRept() {
        return equipmentRept;
    }

    public String getDemandRept() {
        return demandRept;
    }

    public String getWarningRept() {
        return warningRept;
    }

    public String getImgs() {
        return imgs;
    }

    public long getTemplateId() {
        return templateId;
    }

    public long getStuId() {
        return stuId;
    }

    public String getStuName() {
        return stuName;
    }

    public String getStuClass() {
        return stuClass;
    }

    public String getDataRept() {
        return dataRept;
    }

    public String getThinkingRept() {
        return thinkingRept;
    }

    public Date getCreateDate() {
        return createDate;
    }
}