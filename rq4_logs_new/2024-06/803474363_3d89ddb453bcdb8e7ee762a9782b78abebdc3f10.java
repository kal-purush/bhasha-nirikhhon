package unit.test.CollaborativeProjectRequestDAO;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import log.Log;
import mx.fei.coilvicapp.logic.collaborativeprojectrequest.CollaborativeProjectRequest;
import mx.fei.coilvicapp.logic.collaborativeprojectrequest.CollaborativeProjectRequestDAO;
import mx.fei.coilvicapp.logic.course.Course;
import mx.fei.coilvicapp.logic.course.CourseDAO;
import mx.fei.coilvicapp.logic.implementations.DAOException;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import unit.test.Initializer.TestHelper;

public class CollaborativeProjectRequestTest {

    private Course auxCourseOne;
    private Course auxCourseTwo;
    private Course auxCourseThree;
    private Course auxCourseFour;

    private final CollaborativeProjectRequest COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING = new CollaborativeProjectRequest();
    private final CollaborativeProjectRequest AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING = new CollaborativeProjectRequest();
    private final CollaborativeProjectRequestDAO COLLABORATIVE_PROJECT_REQUEST_DAO = new CollaborativeProjectRequestDAO();
    private final TestHelper TEST_HELPER = new TestHelper();

    @Before
    public void setUp() {
        TEST_HELPER.initializeCourses();
        auxCourseOne = TEST_HELPER.getCourseOne();
        auxCourseTwo = TEST_HELPER.getCourseTwo();
        auxCourseThree = TEST_HELPER.getCourseThree();
        auxCourseFour = TEST_HELPER.getCourseFour();
        initializeCollaborativeProjectRequest();
    }

    @After
    public void tearDown() {
        try {
            COLLABORATIVE_PROJECT_REQUEST_DAO.deleteCollaborativeProjectRequestByidCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest());
            COLLABORATIVE_PROJECT_REQUEST_DAO.deleteCollaborativeProjectRequestByidCollaborativeProjectRequest(AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest());
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

    @Test(expected = DAOException.class)
    public void testAcceptCollaborativeProjectRequestFailByCanceledRequestPreviously() throws DAOException {
        if (COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getIdCollaborativeProjectRequest() == 0) {
            COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO
                    .registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        }
        COLLABORATIVE_PROJECT_REQUEST_DAO.cancelCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");

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

        result = COLLABORATIVE_PROJECT_REQUEST_DAO.finalizeCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        System.out.println(result);
        assertTrue(result > 0);

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

    private void initializeAuxCollaborativeProject() {
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestedCourse(auxCourseThree);
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequesterCourse(auxCourseFour);
    }

    @Test
    public void testAreThereAvailableRequestsSuccess() throws DAOException {
        boolean result = false;

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");

        result = COLLABORATIVE_PROJECT_REQUEST_DAO.areThereAvailableRequests(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        System.out.println(result);
        assertTrue(result);

    }

    @Test
    public void testAreThereAvailableRequestsFailByNotAcceptedRequests() throws DAOException {
        boolean result = false;

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));

        result = COLLABORATIVE_PROJECT_REQUEST_DAO.areThereAvailableRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        System.out.println(result);
        assertTrue(!result);
    }

    private String getCurrentTime() {
        LocalDateTime now = LocalDateTime.now();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedDateTime = now.format(formatter);
        return formattedDateTime;
    }

    @Test
    public void testGetCollaborativeProjectByCoursesIdSuccess() throws DAOException {
        CollaborativeProjectRequest result = new CollaborativeProjectRequest();

        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getCollaborativeProjectByCoursesId(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getIdCourse(),
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getIdCourse());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Pendiente");
        assertEquals(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, result);
    }

    @Test
    public void testGetCollaborativeProjectByCoursesIdFailByIncorrectId() throws DAOException {
        CollaborativeProjectRequest result = new CollaborativeProjectRequest();

        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getCollaborativeProjectByCoursesId(0,
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getIdCourse());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Pendiente");
        assertTrue(result.getIdCollaborativeProjectRequest() == 0);
    }

    @Test
    public void testGetAcceptedCollaborativeProjectRequestsByIdProfessorSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getAcceptedCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Aceptado");

        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetAcceptedCollaborativeProjectRequestsByIdProfessorFailByNotAcceptedRequests() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getAcceptedCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Aceptado");

        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetCollaborativeProjectRequestsSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getCollaborativeProjectRequests(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Aceptado");
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Pendiente");
        expected.add(AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetReceivedCollaborativeProjectRequestsSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getReceivedCollaborativeProjectRequests(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Aceptado");
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Pendiente");
        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        expected.add(AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetReceivedCollaborativeProjectRequestsFailByNotReceivedRequests() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));

        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getReceivedCollaborativeProjectRequests(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        System.out.println(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetSentCollaborativeProjectRequestsSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getSentCollaborativeProjectRequests(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Aceptado");
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Pendiente");
        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        expected.add(AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetSentCollaborativeProjectRequestsFailByNotSentRequests() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getSentCollaborativeProjectRequests(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        System.out.println(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetPendingReceivedCollaborativeProjectRequestsByIdProfessorSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getPendingReceivedCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Pendiente");

        expected.add(AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetPendingReceivedCollaborativeProjectRequestsByIdProfessorFailByNotRequests() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getPendingReceivedCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        System.out.println(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetAcceptedReceivedCollaborativeProjectRequestsByIdProfessorSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getAcceptedReceivedCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Aceptado");

        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetAcceptedReceivedCollaborativeProjectRequestsByIdProfessorFailByNotAcceptedRequests() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getAcceptedReceivedCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetRejectedReceivedCollaborativeProjectRequestsByIdProfessorSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Rechazado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getRejectedReceivedCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Rechazado");

        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetRejectedReceivedCollaborativeProjectRequestsByIdProfessorFailByNotRejectedRequests() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getRejectedReceivedCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequestedCourse().getProfessor().getIdProfessor());
        System.out.println(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetPendingSentCollaborativeProjectRequestsByIdProfessorSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getPendingSentCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Pendiente");

        expected.add(AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetPendingSentCollaborativeProjectRequestsByIdProfessorFail() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getPendingSentCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetAcceptedSentCollaborativeProjectRequestsByIdProfessorSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Aceptado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getAcceptedSentCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Aceptado");

        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetAcceptedSentCollaborativeProjectRequestsByIdProfessorFailByNotAcceptedRequests() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getAcceptedSentCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());

        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetRejectedSentCollaborativeProjectRequestsByIdProfessorSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.attendCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING, "Rechazado");
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getRejectedSentCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Rechazado");

        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetRejectedSentCollaborativeProjectRequestsByIdProfessorFailByNotRejectedRequests() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getAcceptedSentCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Rechazado");

        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetCancelledSentCollaborativeProjectRequestsByIdProfessorSuccess() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();
        ArrayList<CollaborativeProjectRequest> expected = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_DAO.cancelCollaborativeProjectRequest(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getCancelledSentCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Cancelado");

        expected.add(COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING);
        assertEquals(expected, result);
    }

    @Test
    public void testGetCancelledSentCollaborativeProjectRequestsByIdProfessorFailByNotCancelledRequests() throws DAOException {
        ArrayList<CollaborativeProjectRequest> result = new ArrayList<>();

        initializeAuxCollaborativeProject();
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setIdCollaborativeProjectRequest(
                COLLABORATIVE_PROJECT_REQUEST_DAO.registerCollaborativeProjectRequest(
                        AUX_COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING));
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setRequestDate(getCurrentTime());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setValidationDate(getCurrentTime());
        result = COLLABORATIVE_PROJECT_REQUEST_DAO.getCancelledSentCollaborativeProjectRequests(
                COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.getRequesterCourse().getProfessor().getIdProfessor());
        COLLABORATIVE_PROJECT_REQUEST_FOR_TESTING.setStatus("Cancelado");
        assertTrue(result.isEmpty());
    }

}