package org.example.module.system.schedulejoblog.domain.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.io.Serializable;
import java.time.LocalDateTime;
/**
 * <p>
 * 系统定时任务执行日志
 * </p>
 *
 * @author linzhaoming
 * @since 2020-11-30
 */
@TableName("t_sys_schedule_job_log")
public class SysScheduleJobLog implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 任务 ID
     */
    private Long jobId;

    /**
     * 任务名称
     */
    private String jobName;

    /**
     * bean 名称
     */
    private String beanName;

    /**
     * 参数
     */
    private String params;

    /**
     * 耗时
     */
    private Integer spendTime;

    /**
     * 消息记录
     */
    private String message;

    /**
     * 状态 [0-失败] [1-成功]
     */
    private Integer status;

    /**
     * 创建时间
     */
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getBeanName() {
        return beanName;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public Integer getSpendTime() {
        return spendTime;
    }

    public void setSpendTime(Integer spendTime) {
        this.spendTime = spendTime;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "SysScheduleJobLog{" +
        ", id=" + id +
        ", jobId=" + jobId +
        ", jobName=" + jobName +
        ", beanName=" + beanName +
        ", params=" + params +
        ", spendTime=" + spendTime +
        ", message=" + message +
        ", status=" + status +
        ", createdAt=" + createdAt +
        "}";
    }
}