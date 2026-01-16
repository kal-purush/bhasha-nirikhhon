package com.delta.cornerstone.cloud.server.domain;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

import javax.persistence.*;
import javax.validation.constraints.*;

import java.io.Serializable;
import java.util.Objects;

import com.delta.cornerstone.cloud.server.domain.enumeration.AppStatus;

/**
 * A CloudApp.
 */
@Entity
@Table(name = "cloud_app")
@Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
public class CloudApp implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sequenceGenerator")
    @SequenceGenerator(name = "sequenceGenerator")
    private Long id;

    @NotNull
    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "description")
    private String description;

    @NotNull
    @Column(name = "epgs", nullable = false)
    private String epgs;

    @Lob
    @Column(name = "configurations")
    private String configurations;

    @Lob
    @Column(name = "upload_file")
    private String uploadFile;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private AppStatus status;

    @NotNull
    @Column(name = "account_name", nullable = false)
    private String accountName;

    @Column(name = "build_file_id")
    private String buildFileId;

    @Lob
    @Column(name = "resouce_mapping")
    private String resouceMapping;

    @Lob
    @Column(name = "build_log")
    private String buildLog;

    @Lob
    @Column(name = "app_content")
    private String appContent;

    // jhipster-needle-entity-add-field - JHipster will add fields here, do not remove
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public CloudApp name(String name) {
        this.name = name;
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public CloudApp description(String description) {
        this.description = description;
        return this;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getEpgs() {
        return epgs;
    }

    public CloudApp epgs(String epgs) {
        this.epgs = epgs;
        return this;
    }

    public void setEpgs(String epgs) {
        this.epgs = epgs;
    }

    public String getConfigurations() {
        return configurations;
    }

    public CloudApp configurations(String configurations) {
        this.configurations = configurations;
        return this;
    }

    public void setConfigurations(String configurations) {
        this.configurations = configurations;
    }

    public String getUploadFile() {
        return uploadFile;
    }

    public CloudApp uploadFile(String uploadFile) {
        this.uploadFile = uploadFile;
        return this;
    }

    public void setUploadFile(String uploadFile) {
        this.uploadFile = uploadFile;
    }

    public AppStatus getStatus() {
        return status;
    }

    public CloudApp status(AppStatus status) {
        this.status = status;
        return this;
    }

    public void setStatus(AppStatus status) {
        this.status = status;
    }

    public String getAccountName() {
        return accountName;
    }

    public CloudApp accountName(String accountName) {
        this.accountName = accountName;
        return this;
    }

    public void setAccountName(String accountName) {
        this.accountName = accountName;
    }

    public String getBuildFileId() {
        return buildFileId;
    }

    public CloudApp buildFileId(String buildFileId) {
        this.buildFileId = buildFileId;
        return this;
    }

    public void setBuildFileId(String buildFileId) {
        this.buildFileId = buildFileId;
    }

    public String getResouceMapping() {
        return resouceMapping;
    }

    public CloudApp resouceMapping(String resouceMapping) {
        this.resouceMapping = resouceMapping;
        return this;
    }

    public void setResouceMapping(String resouceMapping) {
        this.resouceMapping = resouceMapping;
    }

    public String getBuildLog() {
        return buildLog;
    }

    public CloudApp buildLog(String buildLog) {
        this.buildLog = buildLog;
        return this;
    }

    public void setBuildLog(String buildLog) {
        this.buildLog = buildLog;
    }

    public String getAppContent() {
        return appContent;
    }

    public CloudApp appContent(String appContent) {
        this.appContent = appContent;
        return this;
    }

    public void setAppContent(String appContent) {
        this.appContent = appContent;
    }
    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and setters here, do not remove

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CloudApp cloudApp = (CloudApp) o;
        if (cloudApp.getId() == null || getId() == null) {
            return false;
        }
        return Objects.equals(getId(), cloudApp.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }

    @Override
    public String toString() {
        return "CloudApp{" +
            "id=" + getId() +
            ", name='" + getName() + "'" +
            ", description='" + getDescription() + "'" +
            ", epgs='" + getEpgs() + "'" +
            ", configurations='" + getConfigurations() + "'" +
            ", uploadFile='" + getUploadFile() + "'" +
            ", status='" + getStatus() + "'" +
            ", accountName='" + getAccountName() + "'" +
            ", buildFileId='" + getBuildFileId() + "'" +
            ", resouceMapping='" + getResouceMapping() + "'" +
            ", buildLog='" + getBuildLog() + "'" +
            ", appContent='" + getAppContent() + "'" +
            "}";
    }
}