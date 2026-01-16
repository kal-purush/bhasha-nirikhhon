package org.elaastic.qtapi.entity;

import javax.persistence.Transient;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Date;

public class Statement {

    // TODO id ??

    @NotBlank
    private String title;
    @NotBlank
    private String content;

    @NotNull
    private String expectedExplanation;
    @NotNull
    private String choiceSpecification;
    private QuestionType questionType;
    private Statement parentStatement;

    // TODO @Null ??
    private Date dateCreated;
    private Date lastUpdated;

    @NotNull
    private User owner;

    @Transient
    ArrayList<FakeExplanation> fakeExplanations;

//    static constraints = {
//        title blank: false
//        content blank: false
//        questionType nullable: false
//        choiceSpecification nullable: true, validator: { val, obj ->
//            if ((obj.questionType == QuestionType.MultipleChoice || obj.questionType == QuestionType.ExclusiveChoice)
//                    && !val) return ['choiceSpecificationMustBeSet']
//        }
//        parentStatement nullable: true
//        expectedExplanation nullable: true
//    }

//    static transients = ['choiceSpecificationObject', 'fakeExplanations']

    /**
     * Get the list of fake explanations for this statement
     * @return the list of fake explanations
     */
    ArrayList<FakeExplanation> getFakeExplanations() {
        return fakeExplanations;
    }

    // TODO What is a fake axplanation
    public void addFakeExplanation(FakeExplanation fakeExplanation) {
        if (fakeExplanations == null) {
            fakeExplanations = new ArrayList<FakeInteraction>();
        }
        fakeExplanations.add(fakeExplanation);
    }

    // TODO what is this ? and this type
    /**
     * Get the choice specification object
     * @return the choice specification
     */
    ChoiceSpecification getChoiceSpecificationObject() {
        def res = null
        if (choiceSpecification) {
            res = new ChoiceSpecification(choiceSpecification);
        }
        return res;
    }


    /**
     *
     * @return true if statement describes a choice question
     */
    boolean hasChoices() {
        return questionType == QuestionType.ExclusiveChoice || questionType == QuestionType.MultipleChoice;
    }

    /**
     *
     * @return true if statement describes an open-ended question
     */
    boolean isOpenEnded() {
        return questionType == QuestionType.OpenEnded;
    }

    /**
     *
     * @return true if statement describes a multiple choice question
     */
    boolean isMultipleChoice() {
        return questionType == QuestionType.MultipleChoice;
    }

    /**
     *
     * @return true if statement describes an exclusive choice question
     */
    boolean isExclusiveChoice() {
        return questionType == QuestionType.ExclusiveChoice;
    }

    /**
     * Get the attachment
     * @return the attachment
     */
    public Attachement getAttachment() {
        if (id == null) {
            return null;
        }
        Attachement.findByStatement(this); // TODO add list of attachement for Statement
    }
}

enum QuestionType {
    Undefined,
    ExclusiveChoice,
    MultipleChoice,
    OpenEnded
}