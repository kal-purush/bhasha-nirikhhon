package com.tekenable.model;

import javax.persistence.*;
import java.util.Set;

/**
 * Created by smoczyna on 17/03/16.
 * Updated by nbarrett on 19/05/2016:
 * The SpecialistArea is the 'Disease' or 'Patient Condition' within a selected therapeutic area. E.g TherapeuticArea is
 * Cancer, the SpecialistArea can be any one of Lung Cancer, Skin cancer etc.
 * Please Note: This class was originally called TherapeuticArea before the specialistarea was introduced.
 */
@Entity
public class SpecialistArea extends SortEntity {

    private static final long serialVersionUID = 1L;

    //Please Note: Attribute names below in lower case to make for consistent rest url's on the browser.
    //e.g. http://localhost:8080/api/trials/1/specialistarea

    private String title;

    private TherapeuticArea therapeuticarea;

    private Set<QuestionnaireEntry> questionnaireentries;
    private Set<UserSelectorQuestionnaireEntry> userselectorquestionnaireentries;

    private Set<Trial> trials;

    public SpecialistArea() {
    }

    public SpecialistArea(String title, TherapeuticArea therapeuticarea) {

        this.title =title;
        this.therapeuticarea = therapeuticarea;
    }

    public SpecialistArea(String title, TherapeuticArea therapeuticarea, Integer sortOrder) {
        this.title = title;
        this.therapeuticarea = therapeuticarea;
        this.sortOrder = sortOrder;
    }

    @Column(name = "title")
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @ManyToOne
    @JoinColumn(nullable = false, name = "therapeutic_area_id")
    public TherapeuticArea getTherapeuticarea() {
        return therapeuticarea;
    }

    public void setTherapeuticarea(TherapeuticArea therapeuticarea) {
        this.therapeuticarea = therapeuticarea;
    }

    @OneToMany(mappedBy = "specialistarea", cascade = {CascadeType.PERSIST, CascadeType.MERGE}, fetch = FetchType.EAGER)
    public Set<QuestionnaireEntry> getQuestionnaireentries() {
        return questionnaireentries;
    }

    public void setQuestionnaireentries(Set<QuestionnaireEntry> questionnaireentries) {
        this.questionnaireentries = questionnaireentries;
    }

    @OneToMany(mappedBy = "specialistarea", cascade = {CascadeType.ALL}, fetch = FetchType.LAZY)
    public Set<Trial> getTrials() {
        return trials;
    }

    public void setTrials(Set<Trial> trials) {
        this.trials = trials;
    }

    @OneToMany(mappedBy = "specialistarea", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    public Set<UserSelectorQuestionnaireEntry> getUserselectorquestionnaireentries() {
        return userselectorquestionnaireentries;
    }

    public void setUserselectorquestionnaireentries(Set<UserSelectorQuestionnaireEntry> userselectorquestionnaireentries) {
        this.userselectorquestionnaireentries = userselectorquestionnaireentries;
    }

}