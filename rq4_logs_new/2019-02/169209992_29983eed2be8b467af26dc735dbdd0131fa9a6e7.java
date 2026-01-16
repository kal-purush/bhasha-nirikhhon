package org.elaastic.qtapi.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Date;

@Entity
public class Sequance {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;

    @NotNull
    private int rank;

    @NotNull
    private Date dateCreated;
    @NotNull
    private Date lastUpdated;

    @NotNull
    private User owner;
    @NotNull
    private Assignment assignment;
    @NotNull
    private Statement statement;
    @NotNull
    private String executionContext = ExecutionContextType.FaceToFace.name()

    private Interaction activeInteraction;
    @NotNull
    private String state = StateType.beforeStart.toString();
    @NotNull
    private Boolean resultsArePublished; // = false;

    //    {
//        state inList:StateType.values() *.name()
//        executionContext inList:ExecutionContextType.values() *.name()
//    }
//
//    static transients =['interactions','content','title',
//            'responseSubmissionSpecification','evaluationSpecification','responseSubmissionInteraction',
//            'evaluationInteraction','readInteraction','responseRecommendationService']

    @OneToMany(mappedBy = "interaction")
    @JsonIgnore
    ArrayList<Interaction> interactions;

    /**
     * All the interaction of the sequance
     *
     * @return the interactions
     */
    public ArrayList<Interaction> getInteractions() {
        return interactions;
    }

    public void addInteraction(Interaction interaction) {
        if (interactions == null) {
            interactions = new ArrayList<Interaction>();
        interactions.add(interaction);
    }

    /**
     * Get the response submission interaction
     *
     * @return the response submission interaction
     */
    public Interaction getResponseSubmissionInteraction() {
        return interactions != null ? interactions.get(0) : null;
    }

    /**
     * Get the evaluation interaction
     *
     * @return the evaluation interaction
     */
    public Interaction getEvaluationInteraction() {
        return interactions.size() == 3 ? this.interactions.get(1) : null ;
    }

    /**
     * Get the read interaction
     *
     * @return the read interaction
     */
    public Interaction getReadInteraction() {
        return interactions.size() == 3 ? interactions.get(2) : null;
    }

    /**
     * Get the title of the statement
     *
     * @return the title
     */
    String getTitle() {
        statement ?.title
    }

    /**
     * Get the content of the statement
     *
     * @return the content
     */
    String getContent() {
        if ()
    }

    /**
     * Indicate if sequence execution is blended or distance
     *
     * @return true if sequence execution is blended or distance
     */
    boolean executionIsBlendedOrDistance() {
        executionIsBlended() || executionIsDistance()
    }

    /**
     * Indicate if sequence execution is distance
     *
     * @return true if sequence execution is distance
     */
    boolean executionIsDistance() {
        return executionContext == ExecutionContextType.Distance.name();
    }

    /**
     * Indicate if sequence execution is blended
     *
     * @return true if sequence execution is blended
     */
    boolean executionIsBlended() {
        executionContext == ExecutionContextType.Blended.name()
    }

    /**
     * Indicate if sequence execution is face to face
     *
     * @return true if sequence execution is face to face
     */
    boolean executionIsFaceToFace() {
        executionContext == ExecutionContextType.FaceToFace.name()
    }


    DefaultResponseRecommendationService responseRecommendationService

    /**
     * Find all recommended responses for user
     *
     * @param user the user
     * @return the response list
     */
    List<InteractionResponse> findRecommendedResponsesForUser(User user, int attempt =1) {
        Interaction interaction = this.responseSubmissionInteraction;
        def res = []
        if (this.executionIsFaceToFace()) {
            InteractionResponse userResponse = InteractionResponse.findByInteractionAndLearnerAndAttempt(interaction, user, attempt)
            if (userResponse) {
                res = interaction.explanationRecommendationMap()[userResponse.id as String].collect {
                    InteractionResponse.get(it)
                }
            }
        } else {
            def limit = EvaluationSpecification.MAX_RESPONSE_TO_EVALUATE_COUNT
            res = responseRecommendationService.findAllResponsesOrderedByEvaluationCount(interaction, 2, limit)
        }
        res
    }

    /**
     * Find all good responses with explanations
     *
     * @return the good responses
     */
    List<InteractionResponse> findAllGoodResponses(int attempt =1) {
        Interaction interaction = responseSubmissionInteraction
        InteractionResponse.findAllByInteractionAndAttemptAndScore(interaction, attempt, 100f,
                [sort:"meanGrade", order:"desc"])
    }

    /**
     * Find all open responses with explanations
     *
     * @return the open responses
     */
    List<InteractionResponse> findAllOpenResponses(int attempt =1) {
        Interaction interaction = responseSubmissionInteraction
        def res = InteractionResponse.findAllByInteractionAndAttempt(interaction, attempt,
                [sort:"meanGrade", order:"desc"])
        res
    }

    /**
     * Return all bad responses with explanations for the sequence
     * Responses in returned structure can be accessed this way:
     * map[score][answerGroup][index] where
     * score is a Float eg: 100.0, 0.0, -50.0
     * answerGroup is a String eg: "1", "1,3", ""
     * index is an index on the response list for answerGroup eg: 0, 4
     *
     * @param sessionPhase
     * @return
     */
    Map<Float, Map<String, List<InteractionResponse>>> findAllBadResponses(int attempt =1) {
        def list
        Map map = [:]
        Interaction interaction = responseSubmissionInteraction
        list = InteractionResponse.withCriteria {
            eq('interaction', interaction)
            eq('attempt', attempt)
            lt('score', 100.0f)
            order('score', 'desc')
            'learner' {
                order('normalizedUsername', 'asc')
            }
            fetchMode('user', FetchMode.JOIN)
        }
        list.each {
            def answers = it.choiceListSpecification
            if (!map[it.score]) {
                def answerMap = [:]
                answerMap.put(answers,[it])
                map[it.score] = answerMap
            } else {
                if (!map[it.score][answers]) {
                    map[it.score][answers] = [it]
                } else {
                    map[it.score][answers].push(it)
                }
            }
        }
        map
    }

    /**
     * @return true if the sequence has explanations
     */
    boolean hasExplanations() {
        responseSubmissionSpecification ?.studentsProvideExplanation
    }

    /**
     * @return true if the sequence is played with default 3 phases process
     */
    boolean isDefaultProcess() {
        getInteractions() ?.size() == 3
    }

    /**
     * @return true if the sequence is played with short 2 phases process
     */
    boolean isShortProcess() {
        !isDefaultProcess()
    }


    /**
     * @return true if the sequence is stopped
     */
    boolean isStopped() {
        return state == StateType.afterStop.toString();
    }

    boolean isNotStarted() {
        return state == StateType.beforeStart.name();
    }

    /**
     * Indicate if a user has performed evaluation for the current sequence
     *
     * @param user the user
     * @return true if the user has performed evaluation
     */
    boolean userHasPerformedEvaluation(User user) {
        Interaction interaction = this.responseSubmissionInteraction
        def result = PeerGrading.executeQuery(
        "select count(*) from PeerGrading pg where pg.grader = ? and pg.response in (from InteractionResponse resp where resp.interaction = ?)", [
        user, interaction])
        result[0] > 0
    }

    /**
     * Indicate if a user has performed second submission for the current sequence
     *
     * @param user the user
     * @return true if the user has performed second submission
     */
    boolean userHasSubmittedSecondAttempt(User user) {
        responseSubmissionInteraction.hasResponseForUser(user, 2)
    }

    /**
     * Indicate if a user has completed the second phase
     *
     * @param user the user
     * @return true if the user has completed second phase
     */
    boolean userHasCompletedPhase2(User user) {
        def res = false
        if (userHasCompletedPhase1(user)) {
            def userHasPerformedEvaluation = userHasPerformedEvaluation(user)
            def noRecommendedResponses = !findRecommendedResponsesForUser(user)
            if (userHasSubmittedSecondAttempt(user)) {
                res = (userHasPerformedEvaluation || noRecommendedResponses)
            } else if (this.executionIsFaceToFace() && this.statement.isOpenEnded()) {
                res = (userHasPerformedEvaluation || noRecommendedResponses)
            }
        }
        res
    }

    /**
     * Indicate if the sequence allows students to give a second response in a long process
     *
     * @return true if a second response is allowed
     */
    boolean allowsSecondAttemptInLongProcess() {
        !(executionIsFaceToFace() && statement.isOpenEnded())
    }

    /**
     * Indicate if a user has completed the first phase
     *
     * @param user the user
     * @return true if the user has completed first phase
     */
    boolean userHasCompletedPhase1(User user) {
        InteractionResponse.countByInteractionAndAttemptAndLearner(this.responseSubmissionInteraction, 1, user) > 0
    }

    /**
     * Find or create learner sequence
     *
     * @param learner the learner
     */
    private LearnerSequence findOrCreateLearnerSequence(User learner) {
        LearnerSequence ls = LearnerSequence.findByLearnerAndSequence(learner, this)
        if (!ls) {
            ls = new LearnerSequence(learner:learner, sequence:this)
            if (activeInteraction) {
                ls.activeInteraction = responseSubmissionInteraction
            }
            ls.save();
        }
        if (!ls.activeInteraction && activeInteraction) {
            ls.activeInteraction = responseSubmissionInteraction
            ls.save();
        }
        ls
    }


}

public enum StateType {
    beforeStart,
    show,
    afterStop;
}

public enum ExecutionContextType {
    FaceToFace,
    Distance,
    Blended;
}

}