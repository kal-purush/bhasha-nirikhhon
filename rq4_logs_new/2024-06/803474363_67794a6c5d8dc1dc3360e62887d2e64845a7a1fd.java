package unit.test.FeedbackDAO;

import mx.fei.coilvicapp.logic.implementations.DAOException;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import java.util.ArrayList;
import log.Log;
import mx.fei.coilvicapp.logic.collaborativeproject.CollaborativeProject;
import mx.fei.coilvicapp.logic.collaborativeproject.CollaborativeProjectDAO;
import mx.fei.coilvicapp.logic.collaborativeprojectrequest.CollaborativeProjectRequest;
import mx.fei.coilvicapp.logic.collaborativeprojectrequest.CollaborativeProjectRequestDAO;
import mx.fei.coilvicapp.logic.country.Country;
import mx.fei.coilvicapp.logic.country.CountryDAO;
import mx.fei.coilvicapp.logic.course.Course;
import mx.fei.coilvicapp.logic.course.CourseDAO;
import mx.fei.coilvicapp.logic.feedback.FeedbackDAO;
import mx.fei.coilvicapp.logic.feedback.Response;
import mx.fei.coilvicapp.logic.feedback.Question;
import mx.fei.coilvicapp.logic.language.Language;
import mx.fei.coilvicapp.logic.language.LanguageDAO;
import mx.fei.coilvicapp.logic.modality.Modality;
import mx.fei.coilvicapp.logic.modality.ModalityDAO;
import mx.fei.coilvicapp.logic.professor.Professor;
import mx.fei.coilvicapp.logic.professor.ProfessorDAO;
import mx.fei.coilvicapp.logic.term.Term;
import mx.fei.coilvicapp.logic.term.TermDAO;
import mx.fei.coilvicapp.logic.university.University;
import mx.fei.coilvicapp.logic.university.UniversityDAO;
import unit.test.AssignmentDAO.AssignmentRegistrationTest;

/**
 *
 * @author ivanr
 */
public class FeedbackTest {

    private static final LanguageDAO LANGUAGE_DAO = new LanguageDAO();
    private static final Language AUX_LANGUAGE = new Language();

    private static final TermDAO TERM_DAO = new TermDAO();
    private static final Term AUX_TERM = new Term();

    private static final ModalityDAO MODALITY_DAO = new ModalityDAO();
    private static final Modality AUX_MODALITY = new Modality();

    private static final CountryDAO COUNTRY_DAO = new CountryDAO();
    private static final Country AUX_COUNTRY_ONE = new Country();
    private static final Country AUX_COUNTRY_TWO = new Country();

    private static final UniversityDAO UNIVERSITY_DAO = new UniversityDAO();
    private static final University AUX_UNIVERSITY_ONE = new University();
    private static final University AUX_UNIVERSITY_TWO = new University();

    private static final ProfessorDAO PROFESSOR_DAO = new ProfessorDAO();
    private static final Professor AUX_PROFESSOR_ONE = new Professor();
    private static final Professor AUX_PROFESSOR_TWO = new Professor();

    private static final CourseDAO COURSE_DAO = new CourseDAO();
    private static final Course AUX_COURSE_ONE = new Course();
    private static final Course AUX_COURSE_TWO = new Course();

    private static final CollaborativeProjectRequestDAO COLLABORATIVE_PROJECT_REQUEST_DAO = new CollaborativeProjectRequestDAO();
    private static final CollaborativeProjectRequest AUX_COLLABORATIVE_PROJECT_REQUEST = new CollaborativeProjectRequest();

    private static final CollaborativeProjectDAO COLLABORATIVE_PROJECT_DAO = new CollaborativeProjectDAO();
    private static final CollaborativeProject AUX_COLLABORATIVE_PROJECT = new CollaborativeProject();

    private final FeedbackDAO FEEDBACK_DAO = new FeedbackDAO();
    private static final Question QUESTION_FOR_TESTING = new Question();

    private static final ArrayList<Question> QUESTIONS_FOR_TESTING = new ArrayList<>();

    private static final ArrayList<Response> RESPONSES_FOR_TESTING = new ArrayList<>();

    public FeedbackTest() {

    }

    public void initializeQuestion() {
        QUESTION_FOR_TESTING.setQuestionText("¿Recomendarias esta colaboracion?");
        QUESTION_FOR_TESTING.setQuestionType("Estudiante-POST");
    }

    public void initializeStudentQuestions() {
        String questionTexts[] = {"¿Qué te pareció la experiencia?", "¿El profesor cumplió con sus resposabilidades?", "¿Qué fue los mejor de esta experiencia?"};
        String questionTypes[] = {"Estudiante-POST", "Estudiante-POST", "Estudiante-POST"};
        try {
            for (int i = 0; i < 3; i++) {
                Question question = new Question();
                question.setQuestionText(questionTexts[i]);
                question.setQuestionType(questionTypes[i]);
                question.setIdQuestion(FEEDBACK_DAO.registerQuestion(question));
                QUESTIONS_FOR_TESTING.add(question);
            }
        } catch (DAOException exception) {
            Log.getLogger(FeedbackTest.class).error(exception.getMessage(), exception);
        }
    }

    public void initializeProfessorResponses() {
        ArrayList<Response> professorResponses = new ArrayList<>();
        String responseTexts[] = {"Todo salió perfecto", "Cumplió con todas sus actividades", "Conocer nuevos amigos"};

        for (int i = 0; i < 3; i++) {
            Response response = new Response();
            response.setQuestion(QUESTIONS_FOR_TESTING.get(i));
            response.setResponseText(responseTexts[i]);
            response.setIdCollaborativeProject(AUX_COLLABORATIVE_PROJECT.getIdCollaborativeProject());
            response.setIdParticipant(AUX_PROFESSOR_ONE.getIdProfessor());
            RESPONSES_FOR_TESTING.add(response);
        }

    }

    public void initializeProfessorQuestions() {
        String questionTexts[] = {"¿Qué te pareció la experiencia?", "¿El profesor cumplió con sus resposabilidades?", "¿Qué fue los mejor de esta experiencia?"};
        String questionTypes[] = {"Profesor", "Profesor", "Profesor"};
        try {
            for (int i = 0; i < 3; i++) {
                Question question = new Question();
                question.setQuestionText(questionTexts[i]);
                question.setQuestionType(questionTypes[i]);
                question.setIdQuestion(FEEDBACK_DAO.registerQuestion(question));
                QUESTIONS_FOR_TESTING.add(question);
            }
        } catch (DAOException exception) {
            Log.getLogger(FeedbackTest.class).error(exception.getMessage(), exception);
        }
    }

    public void deleteQuestions() {
        try {
            for (int i = 0; i < 3; i++) {
                FEEDBACK_DAO.deleteQuestion(QUESTIONS_FOR_TESTING.get(i));
            }
        } catch (DAOException exception) {
            Log.getLogger(FeedbackTest.class).error(exception.getMessage(), exception);
        }
    }
    
    @Test
    public void testRegisterProfessorResponsesSuccess() {
        int result = 0;
        initializeProfessorQuestions();
        try {
            result = FEEDBACK_DAO.registerProfessorResponses(RESPONSES_FOR_TESTING);
        } catch (DAOException exception) {
            Log.getLogger(FeedbackTest.class).error(exception.getMessage(), exception);
        }
        assertTrue(result > 0);
    }

    @Before
    public void setUp() {
        initializeCountries();
        initializeUniversities();
        initializeProfessors();
        initializeLanguage();
        initializeTerm();
        intializeCourses();
        initializeModality();
        initializeCollaborativeProjectRequest();
        initializeCollaborativeProject();
    }

    @Test
    public void testRegisterQuestionSuccess() {
        int idQuestion = 0;
        initializeStudentQuestions();
        initializeQuestion();
        try {
            idQuestion = FEEDBACK_DAO.registerQuestion(QUESTION_FOR_TESTING);
            QUESTION_FOR_TESTING.setIdQuestion(idQuestion);
        } catch (DAOException exception) {
            Log.getLogger(FeedbackTest.class).error(exception.getMessage(), exception);
        }
        assertTrue(idQuestion > 0);
    }

    @Test
    public void testRegisterQuestionFailByDuplicatedQuestion() {
        int idQuestion = 0;
        initializeStudentQuestions();
        DAOException result = null;
        QUESTION_FOR_TESTING.setQuestionText(QUESTIONS_FOR_TESTING.get(2).getQuestionText());
        QUESTION_FOR_TESTING.setQuestionType(QUESTIONS_FOR_TESTING.get(2).getQuestionType());
        try {
            idQuestion = FEEDBACK_DAO.registerQuestion(QUESTION_FOR_TESTING);
        } catch (DAOException exception) {
            result = exception;
            System.out.println(result.getMessage());
        }
        QUESTION_FOR_TESTING.setIdQuestion(idQuestion);
        assertTrue(result != null);
    }

    @Test
    public void testRegisterResponsesSuccess() {
        int result = 0;

        try {
            result = FEEDBACK_DAO.registerStudentResponses(RESPONSES_FOR_TESTING);
        } catch (DAOException exception) {
            Log.getLogger(FeedbackTest.class).error(exception.getMessage(), exception);
        }
        assertTrue(result > 0);
    }

    @After
    public void tearDown() {
        try {
            deleteQuestions();
            FEEDBACK_DAO.deleteQuestion(QUESTION_FOR_TESTING);
        } catch (DAOException exception) {
            Log.getLogger(FeedbackTest.class).error(exception.getMessage(), exception);
        }
    }

    private void initializeLanguage() {
        AUX_LANGUAGE.setName("Inglés");

        try {
            AUX_LANGUAGE.setIdLanguage(LANGUAGE_DAO.registerLanguage(AUX_LANGUAGE));
        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class).error(exception.getMessage(), exception);
        }
    }

    private void initializeTerm() {
        AUX_TERM.setName("Febrero2024-Junio2024");

        try {
            AUX_TERM.setIdTerm(TERM_DAO.registerTerm(AUX_TERM));
        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class).error(exception.getMessage(), exception);
        }
    }

    private void initializeModality() {
        AUX_MODALITY.setName("Clase espejo");

        try {
            AUX_MODALITY.setIdModality(MODALITY_DAO.registerModality(AUX_MODALITY));
        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class).error(exception.getMessage(), exception);
        }
    }

    private void initializeCountries() {
        AUX_COUNTRY_ONE.setName("México");
        AUX_COUNTRY_TWO.setName("Venezuela");

        try {
            AUX_COUNTRY_ONE.setIdCountry(COUNTRY_DAO.registerCountry(AUX_COUNTRY_ONE));
            AUX_COUNTRY_TWO.setIdCountry(COUNTRY_DAO.registerCountry(AUX_COUNTRY_TWO));
        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class).error(exception.getMessage(), exception);
        }
    }

    private void initializeUniversities() {
        AUX_UNIVERSITY_ONE.setName("Universidad Veracruzana");
        AUX_UNIVERSITY_ONE.setAcronym("UV");
        AUX_UNIVERSITY_ONE.setJurisdiction("Veracruz");
        AUX_UNIVERSITY_ONE.setCity("Xalapa");
        AUX_UNIVERSITY_ONE.setCountry(AUX_COUNTRY_ONE);

        AUX_UNIVERSITY_TWO.setName("Universidad Católica Andrés Bello");
        AUX_UNIVERSITY_TWO.setAcronym("UCAB");
        AUX_UNIVERSITY_TWO.setJurisdiction("Caracas");
        AUX_UNIVERSITY_TWO.setCity("Guayana");
        AUX_UNIVERSITY_TWO.setCountry(AUX_COUNTRY_TWO);
        try {
            AUX_UNIVERSITY_ONE.setIdUniversity(UNIVERSITY_DAO.registerUniversity(AUX_UNIVERSITY_ONE));
            AUX_UNIVERSITY_TWO.setIdUniversity(UNIVERSITY_DAO.registerUniversity(AUX_UNIVERSITY_TWO));
        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class).error(exception.getMessage(), exception);
        }
    }

    private void initializeProfessors() {
        AUX_PROFESSOR_ONE.setName("Maria");
        AUX_PROFESSOR_ONE.setPaternalSurname("Arenas");
        AUX_PROFESSOR_ONE.setMaternalSurname("Valdes");
        AUX_PROFESSOR_ONE.setEmail("aaren@uv.mx");
        AUX_PROFESSOR_ONE.setGender("Mujer");
        AUX_PROFESSOR_ONE.setPhoneNumber("1234567890");
        AUX_PROFESSOR_ONE.setUniversity(AUX_UNIVERSITY_ONE);

        AUX_PROFESSOR_TWO.setName("Jorge");
        AUX_PROFESSOR_TWO.setPaternalSurname("Ocharan");
        AUX_PROFESSOR_TWO.setMaternalSurname("Hernandez");
        AUX_PROFESSOR_TWO.setEmail("jochar@uv.mx");
        AUX_PROFESSOR_TWO.setGender("Hombre");
        AUX_PROFESSOR_TWO.setPhoneNumber("1234567890");
        AUX_PROFESSOR_TWO.setUniversity(AUX_UNIVERSITY_TWO);
        try {
            AUX_PROFESSOR_ONE.setIdProfessor(PROFESSOR_DAO.registerProfessor(AUX_PROFESSOR_ONE));
            AUX_PROFESSOR_TWO.setIdProfessor(PROFESSOR_DAO.registerProfessor(AUX_PROFESSOR_TWO));
        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class).error(exception.getMessage(), exception);
        }
    }

    private void intializeCourses() {
        AUX_COURSE_ONE.setName("Programacion");
        AUX_COURSE_ONE.setGeneralObjective("Los alumnos comprendan la"
                + " programacion estructurada y orientada a objetos");
        AUX_COURSE_ONE.setTopicsInterest("Abstraccion, Herencia, Polimorfismo");
        AUX_COURSE_ONE.setNumberStudents(35);
        AUX_COURSE_ONE.setStudentsProfile("Ingenieria de software");
        AUX_COURSE_ONE.setTerm(AUX_TERM);
        AUX_COURSE_ONE.setLanguage(AUX_LANGUAGE);
        AUX_COURSE_ONE.setAdditionalInformation("Ademas de la programacion"
                + " orientada a objetos veremos el paradigma funcional");
        AUX_COURSE_ONE.setProfessor(AUX_PROFESSOR_ONE);

        AUX_COURSE_TWO.setName("Bases de Datos");
        AUX_COURSE_TWO.setGeneralObjective("Los alumnos comprendan el diseño, "
                + "implementación y administración de bases de datos relacionales.");
        AUX_COURSE_TWO.setTopicsInterest("Modelado de datos, SQL, Normalización");
        AUX_COURSE_TWO.setNumberStudents(40);
        AUX_COURSE_TWO.setStudentsProfile("Ingeniería en Sistemas Computacionales");
        AUX_COURSE_TWO.setTerm(AUX_TERM);
        AUX_COURSE_TWO.setLanguage(AUX_LANGUAGE);
        AUX_COURSE_TWO.setAdditionalInformation("Además de bases de datos relacionales, "
                + "se introducirá a bases de datos NoSQL y técnicas avanzadas de optimización.");
        AUX_COURSE_TWO.setProfessor(AUX_PROFESSOR_TWO);
        try {
            AUX_COURSE_TWO.setIdCourse(COURSE_DAO.registerCourse(AUX_COURSE_TWO));
            AUX_COURSE_ONE.setIdCourse(COURSE_DAO.registerCourse(AUX_COURSE_ONE));
            COURSE_DAO.evaluateCourseProposal(AUX_COURSE_ONE, "Aceptado");
            COURSE_DAO.evaluateCourseProposal(AUX_COURSE_TWO, "Aceptado");
        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class).error(exception.getMessage(), exception);
        }
        AUX_COURSE_TWO.setStatus("Aceptado");
        AUX_COURSE_ONE.setStatus("Aceptado");
    }

    private void initializeCollaborativeProjectRequest() {

        AUX_COLLABORATIVE_PROJECT_REQUEST.setRequestedCourse(AUX_COURSE_ONE);
        AUX_COLLABORATIVE_PROJECT_REQUEST.setRequesterCourse(AUX_COURSE_TWO);

        try {

            AUX_COLLABORATIVE_PROJECT_REQUEST.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(AUX_COLLABORATIVE_PROJECT_REQUEST));
            COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(AUX_COLLABORATIVE_PROJECT_REQUEST, "Aceptado");
        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class).error(exception.getMessage(), exception);
        }
        AUX_COLLABORATIVE_PROJECT_REQUEST.setStatus("Aceptado");
    }

    private void initializeCollaborativeProject() {
        AUX_COLLABORATIVE_PROJECT.setName("Programación y Bases de Datos");
        AUX_COLLABORATIVE_PROJECT.setStatus("Aceptado");
        AUX_COLLABORATIVE_PROJECT.setDescription("Este proyecto combina los conocimientos de programación orientada a objetos y bases de datos "
                + "para desarrollar una aplicación completa que gestione información de manera eficiente.");
        AUX_COLLABORATIVE_PROJECT.setGeneralObjective("Integrar conceptos de programación y bases de datos para desarrollar una solución software completa.");
        AUX_COLLABORATIVE_PROJECT.setModality(AUX_MODALITY);
        AUX_COLLABORATIVE_PROJECT.setCode("ProgBasdat2024!");
        AUX_COLLABORATIVE_PROJECT.setSyllabusPath("/syllabus/proyecto_integrador.pdf");
        AUX_COLLABORATIVE_PROJECT.setRequestedCourse(AUX_COURSE_ONE);
        AUX_COLLABORATIVE_PROJECT.setRequesterCourse(AUX_COURSE_TWO);
        try {
            AUX_COLLABORATIVE_PROJECT.setIdCollaborativeProject(COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject(AUX_COLLABORATIVE_PROJECT, AUX_COLLABORATIVE_PROJECT_REQUEST));
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(AUX_COLLABORATIVE_PROJECT, "Aceptado");
        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class).error(exception.getMessage(), exception);
        }
        AUX_COLLABORATIVE_PROJECT_REQUEST.setStatus("Aceptado");
    }
}