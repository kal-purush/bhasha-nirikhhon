package org.elaastic.qtapi.Entity;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.validation.constraints.NotNull;
import java.util.Date;

public class PeerGrading {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;


    private Date dateCreated;
    private Date lastUpdated;

    private Float grade; // Can be null
    private String annotation; // Can be null

    @NotNull
    private User grader;
    @NotNull
    private InteractionResponse response;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Date getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public Float getGrade() {
        return grade;
    }

    public void setGrade(Float grade) {
        this.grade = grade;
    }

    public String getAnnotation() {
        return annotation;
    }

    public void setAnnotation(String annotation) {
        this.annotation = annotation;
    }

    public User getGrader() {
        return grader;
    }

    public void setGrader(User grader) {
        this.grader = grader;
    }

    public InteractionResponse getResponse() {
        return response;
    }

    public void setResponse(InteractionResponse response) {
        this.response = response;
    }
}