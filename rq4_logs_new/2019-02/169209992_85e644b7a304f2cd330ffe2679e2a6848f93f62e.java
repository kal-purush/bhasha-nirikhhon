package org.elaastic.qtapi.entity;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.validation.constraints.NotNull;
import java.util.Date;

public class Interaction {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;

    @NotNull
    private String interactionType;

    @NotNull
    private Integer rank;

    @NotNull
    private String specification;

    @NotNull
    private Date dateCreated;
    @NotNull
    private Date lastUpdate;

    @NotNull
    private User owner;
    @NotNull
    private Sequence sequance;
    @NotNull
    private String state;

    private String result;
    private String explanationRecommendationmapping;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getInteractionType() {
        return interactionType;
    }

    public void setInteractionType(String interactionType) {
        this.interactionType = interactionType;
    }

    public Integer getRank() {
        return rank;
    }

    public void setRank(Integer rank) {
        this.rank = rank;
    }

    public String getSpecification() {
        return specification;
    }

    public void setSpecification(String specification) {
        this.specification = specification;
    }

    public Date getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public User getOwner() {
        return owner;
    }

    public void setOwner(User owner) {
        this.owner = owner;
    }

    public Sequence getSequance() {
        return sequance;
    }

    public void setSequance(Sequence sequance) {
        this.sequance = sequance;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getExplanationRecommendationmapping() {
        return explanationRecommendationmapping;
    }

    public void setExplanationRecommendationmapping(String explanationRecommendationmapping) {
        this.explanationRecommendationmapping = explanationRecommendationmapping;
    }
}