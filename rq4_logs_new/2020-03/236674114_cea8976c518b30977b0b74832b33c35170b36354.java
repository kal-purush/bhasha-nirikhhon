
package gt.edu.usac.cunoc.ingenieria.eps.configuration.repository;

import static gt.edu.usac.cunoc.ingenieria.eps.configuration.Constants.PERSISTENCE_UNIT_NAME;
import java.time.LocalDate;
import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;


@Singleton
@Startup
public class PropertyRepository {
    
    public static Integer MAXIMUM_SUPERVISOR_WORKLOAD;
    public static Integer VALIDATION_PERCENTAGE_EXTENSION;
    public static Integer REVIEW_TIME_DAYS;
    public static Integer TIME_OF_PROCESS_WITHOUT_MOVEMENT;
    public static LocalDate DEADLINE_TO_SUBMIT_DOCUMENT;
    public static Integer MINIMUN_EXECUTION_MONTHS;
    public static Integer MAXIMUN_EXECUTION_MONTHS;
    public static Integer CHARACTER_LIMIT_TITLE;
    public static Integer LIMIT_GENERAL_OBJECTIVE;
    public static Integer LIMIT_SPECIFIC_OBJECTIVE;
    public static Integer CHARACTER_LIMIT_JUSTIFICATION;
    public static LocalDate GENERAL_LIMIT_RECEPTION_DATE;
    
    private EntityManager entityManager;
    
    private final Query queryMaximunSupervisorWorload = entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = MAXIMUM_SUPERVISOR_WORKLOAD");
    private final Query queryValidationPercentageExtension =  entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = VALIDATION_PERCENTAGE_EXTENSION");
    private final Query queryReviewsDays =  entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = REVIEW_TIME_DAYS");
    private final Query queryTimeProcessWithoutMovement =  entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = TIME_OF_PROCESS_WITHOUT_MOVEMENT");
    private final Query queryDeadlineSubmitDocument =  entityManager.createQuery("SELECT valueDate FROM PROPERTY WHERE name = DEADLINE_TO_SUBMIT_DOCUMENT");
    private final Query queryMinimunExecutionMonths =  entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = MINIMUN_EXECUTION_MONTHS");
    private final Query queryMaximunExecutionMonth =  entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = MAXIMUN_EXECUTION_MONTHS");
    private final Query queryCharacterLimitTitle =  entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = CHARACTER_LIMIT_TITLE");
    private final Query queryLimitGeneralObjective =  entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = LIMIT_GENERAL_OBJECTIVE");
    private final Query queryLimitSpecificObjective =  entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = LIMIT_SPECIFIC_OBJECTIVE");
    private final Query queryCharacterLimitJustification =  entityManager.createQuery("SELECT valueInt FROM PROPERTY WHERE name = CHARACTER_LIMIT_JUSTIFICATION");
    private final Query queryGeneralLimitReceptionDate =  entityManager.createQuery("SELECT valueDate FROM PROPERTY WHERE name = GENERAL_LIMIT_RECEPTION_DATE");
     
    @PostConstruct
    public void initialize(){
        MAXIMUM_SUPERVISOR_WORKLOAD = (Integer) queryMaximunSupervisorWorload.getSingleResult();
        VALIDATION_PERCENTAGE_EXTENSION = (Integer) queryValidationPercentageExtension.getSingleResult();
        REVIEW_TIME_DAYS = (Integer) queryReviewsDays.getSingleResult();
        TIME_OF_PROCESS_WITHOUT_MOVEMENT = (Integer) queryTimeProcessWithoutMovement.getSingleResult();
        DEADLINE_TO_SUBMIT_DOCUMENT = (LocalDate) queryDeadlineSubmitDocument.getSingleResult();
        MINIMUN_EXECUTION_MONTHS = (Integer) queryMinimunExecutionMonths.getSingleResult();
        MAXIMUN_EXECUTION_MONTHS = (Integer) queryMaximunExecutionMonth.getSingleResult();
        CHARACTER_LIMIT_TITLE = (Integer) queryCharacterLimitTitle.getSingleResult();
        LIMIT_GENERAL_OBJECTIVE = (Integer) queryLimitGeneralObjective.getSingleResult();
        LIMIT_SPECIFIC_OBJECTIVE = (Integer) queryLimitSpecificObjective.getSingleResult();
        CHARACTER_LIMIT_JUSTIFICATION = (Integer) queryCharacterLimitJustification.getSingleResult();
        GENERAL_LIMIT_RECEPTION_DATE = (LocalDate) queryGeneralLimitReceptionDate.getSingleResult();
    }

    @PersistenceContext(name = PERSISTENCE_UNIT_NAME)
    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }
    
    
}