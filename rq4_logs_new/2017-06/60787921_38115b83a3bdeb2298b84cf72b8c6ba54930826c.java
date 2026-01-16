package dataBean.dubbo.projectSys;

import dataBean.IDataBean;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by alex on 2017/6/8.
 */
public class MyProjectAuditDto implements Serializable , IDataBean{

    private String id;

    private static final long serialVersionUID = 4235219924865703315L;
    /**
     * 项目编号
     */
    private String  projectCode;
    /**
     * 审核状态
     * @see com.miaoqian.proj.api.enums.ProjectAuditStatus
     */
    private String auditStatus;
    /**
     * 审核人
     */
    private String crateName;
    /**
     * 审核时间
     */
    private Date createTime;
    /**
     * 审核人ip
     */
    private String creatorIP;

    @Override
    public String toString() {
        return "ProjectAuditDto{" +
                "projectCode='" + projectCode + '\'' +
                ", auditStatus='" + auditStatus + '\'' +
                ", crateName='" + crateName + '\'' +
                ", createTime=" + createTime +
                ", creatorIP='" + creatorIP + '\'' +
                '}';
    }

    /**
     * 项目编号
     * @return
     */
    public String getProjectCode() {
        return projectCode;
    }

    /**
     * 项目编号
     * @param projectCode
     */
    public void setProjectCode(String projectCode) {
        this.projectCode = projectCode;
    }

    /**
     * 审核状态
     * @return
     */
    public String getAuditStatus() {
        return auditStatus;
    }

    /**
     * 审核状态
     * @param auditStatus
     */
    public void setAuditStatus(String auditStatus) {
        this.auditStatus = auditStatus;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCrateName() {
        return crateName;
    }

    public void setCrateName(String crateName) {
        this.crateName = crateName;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getCreatorIP() {
        return creatorIP;
    }

    public void setCreatorIP(String creatorIP) {
        this.creatorIP = creatorIP;
    }
}