package com.beroe.live.services.usermanagement.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModel;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

import javax.persistence.*;
import javax.validation.constraints.*;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.Objects;

/**
 * Entity CompanyInfo
 * It represents companies representing suppliers as well as procurement user company
 */
@ApiModel(description = "Entity CompanyInfo It represents companies representing suppliers as well as procurement user company")
@Entity
@Table(name = "company")
@Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
public class Company implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotNull
    @Column(name = "company_name", nullable = false)
    private String companyName;

    @OneToMany(mappedBy = "company")
    @Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
    private Set<CompanyDomain> companyDomains = new HashSet<>();
    @OneToOne(mappedBy = "company")
    @JsonIgnore
    private UserProfile userProfile;

    @OneToOne(mappedBy = "company")
    @JsonIgnore
    private Invitation invitation;

    // jhipster-needle-entity-add-field - JHipster will add fields here, do not remove
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCompanyName() {
        return companyName;
    }

    public Company companyName(String companyName) {
        this.companyName = companyName;
        return this;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public Set<CompanyDomain> getCompanyDomains() {
        return companyDomains;
    }

    public Company companyDomains(Set<CompanyDomain> companyDomains) {
        this.companyDomains = companyDomains;
        return this;
    }

    public Company addCompanyDomain(CompanyDomain companyDomain) {
        this.companyDomains.add(companyDomain);
        companyDomain.setCompany(this);
        return this;
    }

    public Company removeCompanyDomain(CompanyDomain companyDomain) {
        this.companyDomains.remove(companyDomain);
        companyDomain.setCompany(null);
        return this;
    }

    public void setCompanyDomains(Set<CompanyDomain> companyDomains) {
        this.companyDomains = companyDomains;
    }

    public UserProfile getUserProfile() {
        return userProfile;
    }

    public Company userProfile(UserProfile userProfile) {
        this.userProfile = userProfile;
        return this;
    }

    public void setUserProfile(UserProfile userProfile) {
        this.userProfile = userProfile;
    }

    public Invitation getInvitation() {
        return invitation;
    }

    public Company invitation(Invitation invitation) {
        this.invitation = invitation;
        return this;
    }

    public void setInvitation(Invitation invitation) {
        this.invitation = invitation;
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
        Company company = (Company) o;
        if (company.getId() == null || getId() == null) {
            return false;
        }
        return Objects.equals(getId(), company.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }

    @Override
    public String toString() {
        return "Company{" +
            "id=" + getId() +
            ", companyName='" + getCompanyName() + "'" +
            "}";
    }
}