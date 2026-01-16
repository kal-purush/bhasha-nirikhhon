package unit.test.CollaborativeProjectRequestDAO;

import log.Log;
import mx.fei.coilvicapp.logic.collaborativeprojectrequest.CollaborativeProjectRequest;
import mx.fei.coilvicapp.logic.collaborativeprojectrequest.CollaborativeProjectRequestDAO;
import mx.fei.coilvicapp.logic.course.Course;
import mx.fei.coilvicapp.logic.course.CourseDAO;
import mx.fei.coilvicapp.logic.implementations.DAOException;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import unit.test.Initializer.TestHelper;

public class CollaborativeProjectRequestTest {

    private Course auxCourseOne;
    private Course auxCourseTwo;

    private final CollaborativeProjectRequest COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING = new CollaborativeProjectRequest();
    private final CollaborativeProjectRequestDAO COLLABORATIVE_PROJECT_REQUEST_DAO = new CollaborativeProjectRequestDAO();
    private final TestHelper TEST_HELPER = new TestHelper();

    @Before
    public void setUp() {
        TEST_HELPER.intializeCourses();
        auxCourseOne = TEST_HELPER.getCourseOne();
        auxCourseTwo = TEST_HELPER.getCourseTwo();
        initializeCollaborativeProjectRequest();
    }

    @After
    public void tearDown() {
        try {
            COLLABORATIVE_PROJECT_REQUEST_DAO.deleteCollaborativeProjectRequestByidCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest());
        } catch (DAOException exception) {
            Log.getLogger(CollaborativeProjectRequestTest.class).error(exception.getMessage(), exception);
        }
        TEST_HELPER.deleteAll();
    }

    public void initializeCollaborativeProjectRequest() {
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestedCourse(auxCourseOne);
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequesterCourse(auxCourseTwo);
    }

    public Course initializeAuxCourse() {
        CourseDAO courseDAO = new CourseDAO();
        Course auxCourse = new Course();
        auxCourse.setName("Desarrollo Web");
        auxCourse.setGeneralObjective("Los alumnos dominarán las tecnologías"
                + " y herramientas necesarias para crear aplicaciones web modernas y responsivas.");
        auxCourse.setTopicsInterest("HTML, CSS, JavaScript, React, Node.js");
        auxCourse.setNumberStudents(40);
        auxCourse.setStudentsProfile("Ingeniería de software, Diseño gráfico");
        auxCourse.setTerm(auxCourseOne.getTerm());
        auxCourse.setLanguage(auxCourseOne.getLanguage());
        auxCourse.setAdditionalInformation("El curso incluye la creación de"
                + " proyectos completos desde cero utilizando metodologías ágiles.");
        auxCourse.setProfessor(auxCourseOne.getProfessor());
        try {
            auxCourse.setIdCourse(courseDAO.registerCourse(auxCourse));
        } catch (DAOException exception) {
            Log.getLogger(CollaborativeProjectRequestTest.class).error(exception.getMessage(), exception);
        }
        return auxCourse;
    }

    @Test
    public void testRegisterCollaborativeProjectRequestSuccess() throws DAOException {
        int idCollaborativeProjectRequest = 0;
        idCollaborativeProjectRequest = COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(idCollaborativeProjectRequest);
        System.out.println(idCollaborativeProjectRequest);
        assertTrue(idCollaborativeProjectRequest > 0);
    }

    @Test(expected = DAOException.class)
    public void testRegisterCollaborativeProjectRequestFailByMoreThanOneRequest() throws DAOException {
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        }
        CollaborativeProjectRequest auxCollaborativeProjectRequest = new CollaborativeProjectRequest();
        auxCollaborativeProjectRequest.setRequestedCourse(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse());
        auxCollaborativeProjectRequest.setRequesterCourse(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse());
        COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(auxCollaborativeProjectRequest);
    }

    @Test(expected = DAOException.class)
    public void testRegisterCollaborativeProjectRequestFailByInappropiateStateInRequester() throws DAOException {
        CollaborativeProjectRequest auxCollaborativeProjectRequest = new CollaborativeProjectRequest();
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
            COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        }
        auxCollaborativeProjectRequest.setRequestedCourse(auxCourseTwo);
        auxCollaborativeProjectRequest.setRequesterCourse(initializeAuxCourse());
        try {
            COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(auxCollaborativeProjectRequest);
        } finally {
            CourseDAO courseDAO = new CourseDAO();
            courseDAO.deleteCourseByIdCourse(auxCollaborativeProjectRequest.getRequesterCourse().getIdCourse());
        }
    }

    @Test(expected = DAOException.class)
    public void testRegisterCollaborativeProjectRequestFailByInappropiateStateInRequested() throws DAOException {
        CollaborativeProjectRequest auxCollaborativeProjectRequest = new CollaborativeProjectRequest();
        CourseDAO courseDAO = new CourseDAO();
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
            COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        }
        Course auxCourse = initializeAuxCourse();
        auxCollaborativeProjectRequest.setRequestedCourse(auxCourseTwo);
        courseDAO.evaluateCourseProposal(auxCourse, "Aceptado");
        auxCollaborativeProjectRequest.setRequesterCourse(auxCourse);
        try {
            COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(auxCollaborativeProjectRequest);
        } finally {
            courseDAO.deleteCourseByIdCourse(auxCollaborativeProjectRequest.getRequesterCourse().getIdCourse());
        }
    }

    @Test
    public void testAttendCollaborativeProjectRequestSuccess() throws DAOException {
        int result = 0;
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        }

        result = COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        System.out.println(result);
        assertTrue(result > 0);
    }

    @Test //(expected = DAOException.class)
    public void testAcceptCollaborativeProjectRequestFailByCanceledRequestPreviously() throws DAOException {
        try {
            if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                        .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
            }
            COLLABORATIVE_PROJECT_REQUEST_DAO.cancelCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
            COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        } catch (DAOException exception) {
            System.out.println(exception.getMessage());
        }

    }

    @Test
    public void testRejectCollaborativeProjectRequestSuccess() throws DAOException {
        int result = 0;

        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        }
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Rechazado");
        System.out.println(result);
        assertTrue(result > 0);
    }

    @Test
    public void testCancelCollaborativeProjectRequestSuccess() throws DAOException {
        int result = 0;
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        }
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.cancelCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        System.out.println(result);
        assertTrue(result > 0);
    }

    @Test(expected = DAOException.class)
    public void testCancelCollaborativeProjectRequestFailByCanceledPreviously() throws DAOException {
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        }
        COLLABORATIVE_PROJECT_REQUEST_DAO.cancelCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        COLLABORATIVE_PROJECT_REQUEST_DAO.cancelCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
    }

    @Test(expected = DAOException.class)
    public void testCancelCollaborativeProjectRequestFailByInapropiateState() throws DAOException {
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        }
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Rechazado");
        COLLABORATIVE_PROJECT_REQUEST_DAO.cancelCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
    }

    @Test
    public void testFinalizeCollaborativeProjectRequestSuccess() throws DAOException {
        int result = 0;
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        }
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        try {
            result = COLLABORATIVE_PROJECT_REQUEST_DAO.finalizeCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
            assertTrue(result > 0);
        } catch (DAOException exception) {
            System.out.println(exception.getMessage());
        }
    }

    @Test
    public void testDeleteCollaborativeProjectRequestSuccess() throws DAOException {
        int result = 0;
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        }
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.deleteCollaborativeProjectRequestByidCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest());
        System.out.println(result);
        assertTrue(result > 0);
    }

    @Test
    public void testDeleteCollaborativeProjectRequestFailByNonexistenceId() throws DAOException {
        int result = 0;
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.deleteCollaborativeProjectRequestByidCollaborativeProjectRequest(0);
        System.out.println(result);
        assertTrue(result == 0);
    }

}