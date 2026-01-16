package unit.test.CollaborativeProjectDAO;

import java.util.ArrayList;
import log.Log;
import mx.fei.coilvicapp.logic.collaborativeproject.*;
import mx.fei.coilvicapp.logic.collaborativeprojectrequest.CollaborativeProjectRequest;
import mx.fei.coilvicapp.logic.course.Course;
import mx.fei.coilvicapp.logic.implementations.DAOException;
import mx.fei.coilvicapp.logic.professor.Professor;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import unit.test.Initializer.TestHelper;



public class CollaborativeProjectTest {
    
    private final CollaborativeProject COLLABORATIVE_PROJECT_FOR_TESTING = new CollaborativeProject();
    private final ArrayList<CollaborativeProject> COLLABORATIVE_PROJECTS_FOR_TESTING = new ArrayList<>();
    private final CollaborativeProjectDAO COLLABORATIVE_PROJECT_DAO = new CollaborativeProjectDAO();
    private CollaborativeProjectRequest auxCollaborativeProjectRequest;    
    private final TestHelper testHelper = new TestHelper();
        
    
    public void setUp() {        
        testHelper.initializeCollaborativeProjectRequest();        
        testHelper.initializeModality();
        auxCollaborativeProjectRequest = testHelper.getCollaborativeProjectRequest();
        initializeCollaborativeProject();
    }
    
    private void initializeCollaborativeProject() {
        COLLABORATIVE_PROJECT_FOR_TESTING.setName("Programación y Bases de Datos");
        COLLABORATIVE_PROJECT_FOR_TESTING.setStatus("Pendiente");
        COLLABORATIVE_PROJECT_FOR_TESTING.setDescription("Este proyecto combina los conocimientos de"
        + " programación orientada a objetos y bases de datos "
        + "para desarrollar una aplicación completa que gestione información de manera eficiente.");
        COLLABORATIVE_PROJECT_FOR_TESTING.setGeneralObjective("Integrar conceptos de programación y"
        + " bases de datos para desarrollar una solución software completa.");
        COLLABORATIVE_PROJECT_FOR_TESTING.setModality(testHelper.getModality());
        COLLABORATIVE_PROJECT_FOR_TESTING.setCode("ProgBasdat2025!");
        COLLABORATIVE_PROJECT_FOR_TESTING.setSyllabusPath("/syllabus/proyecto_integrador.pdf");
        COLLABORATIVE_PROJECT_FOR_TESTING.setRequesterCourse(testHelper.getCourseOne());
        COLLABORATIVE_PROJECT_FOR_TESTING.setRequestedCourse(testHelper.getCourseTwo());
    }
    
    private void initializeCollaborativeProjects() {
        setUp();
        CollaborativeProject collaborativeProject = new CollaborativeProject();
        int idCollaborativeProject;
        collaborativeProject.setName("Desarrollo de Aplicaciones Móviles");
        collaborativeProject.setStatus("En Proceso");
        collaborativeProject.setDescription("Este proyecto involucra la creación de aplicaciones móviles"
        + " utilizando tecnologías modernas para ofrecer funcionalidades avanzadas y una experiencia de usuario óptima.");
        collaborativeProject.setGeneralObjective("Desarrollar habilidades en el diseño y la implementación de aplicaciones móviles"
        + " mediante el uso de frameworks y herramientas actuales.");
        collaborativeProject.setModality(testHelper.getModality());
        collaborativeProject.setCode("DesAppMov2024!");
        collaborativeProject.setSyllabusPath("/syllabus/proyecto_desarrollo_apps_moviles.pdf");
        collaborativeProject.setRequesterCourse(testHelper.getCourseThree());
        collaborativeProject.setRequestedCourse(testHelper.getCourseFour());
        COLLABORATIVE_PROJECTS_FOR_TESTING.add(COLLABORATIVE_PROJECT_FOR_TESTING);        
        COLLABORATIVE_PROJECTS_FOR_TESTING.add(collaborativeProject);        
        try {
            idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
            (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
            COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
            idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
            (collaborativeProject, testHelper.getCollaborativeProjectRequestTwo());
            collaborativeProject.setIdCollaborativeProject(idCollaborativeProject);
        } catch (DAOException exception) {
            Log.getLogger(CollaborativeProjectTest.class).error(exception.getMessage(), exception);
        }                
    }
    
    private void deleteCollaborativeProjects() {
        try {
            for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
                COLLABORATIVE_PROJECT_DAO.deleteCollaborativeProjectByidCollaborativeProject
                (collaborativeProject.getIdCollaborativeProject());
            }
        } catch (DAOException exception) {
            Log.getLogger(CollaborativeProjectTest.class).error(exception.getMessage(), exception);
        }
    }       
    
    @Test
    public void registerCollaborativeProjectSucces() {
        setUp();
        int idCollaborativeProject = 0;
        try {
            idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
            (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
            COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        } catch (DAOException exception) {
            Log.getLogger(CollaborativeProjectTest.class).error(exception.getMessage(), exception);
        }
        assertTrue(idCollaborativeProject > 0);
    }
    
    @Test
    public void registerCollaborativeProjectFailByRejectedCollaborativeProjectRequest() {
        testHelper.initializeRejectedCollaborativeProjectRequest();
        testHelper.initializeModality();
        auxCollaborativeProjectRequest = testHelper.getRejectedCollaborativeProjectRequest();
        initializeCollaborativeProject();
        DAOException exception =
        assertThrows(DAOException.class, () -> COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest));
        System.out.println(exception.getMessage());        
    }
    
    @Test
    public void registerCollaborativeProjectFailByCoursesInAnotherCollaborativeProject() {        
        testHelper.initializeCollaborativeProject();
        initializeCollaborativeProject();
        auxCollaborativeProjectRequest = testHelper.getCollaborativeProjectRequest();
        DAOException exception =
        assertThrows(DAOException.class, () -> COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest));        
        System.out.println(exception.getMessage());
    }
    
    @Test
    public void registerCollaborativeProjectFailByDuplicateCode() {
        testHelper.initializeCollaborativeProject();
        initializeCollaborativeProject();
        COLLABORATIVE_PROJECT_FOR_TESTING.setCode(testHelper.getCollaborativeProject().getCode());
        auxCollaborativeProjectRequest = testHelper.getCollaborativeProjectRequestTwo();
        DAOException exception = 
        assertThrows(DAOException.class, () -> COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest));        
        System.out.println(exception.getMessage());
    }
    
    @Test
    public void acceptCollaborativeProjectProposalSucces() throws DAOException{
        setUp();
        int idCollaborativeProject;
        String status;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(COLLABORATIVE_PROJECT_FOR_TESTING, "Aceptado");
        status = COLLABORATIVE_PROJECT_DAO.getCollaborativeProjectByIdCollaborativeProject(idCollaborativeProject).getStatus();
        assertEquals("Aceptado",status);
    }

    @Test
    public void rejectCollaborativeProjectProposalSucces() throws DAOException {
        setUp();
        int idCollaborativeProject;
        String status;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal
        (COLLABORATIVE_PROJECT_FOR_TESTING, "Rechazado");
        status = COLLABORATIVE_PROJECT_DAO.getCollaborativeProjectByIdCollaborativeProject(idCollaborativeProject).getStatus();
        assertEquals("Rechazado",status);
    }
    
    @Test
    public void evaluateCollaborativeProjectProposalFailByAlreadyEvaluatedCollaborativeProject() throws DAOException {
        initializeCollaborativeProject();
        DAOException exception = assertThrows
        (DAOException.class, () -> COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal
        (COLLABORATIVE_PROJECT_FOR_TESTING, "Rechazado"));        
        System.out.println(exception.getMessage());
    }
        
    @Test
    public void getCollaborativeProjectByCodeSucces() throws DAOException {
        setUp();
        CollaborativeProject collaborativeProject;
        int idCollaborativeProject;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        collaborativeProject = COLLABORATIVE_PROJECT_DAO.getCollaborativeProjectByCode(COLLABORATIVE_PROJECT_FOR_TESTING.getCode());                        
        assertEquals(COLLABORATIVE_PROJECT_FOR_TESTING,collaborativeProject);
    }
    
    @Test 
    public void getCollaborativeProjectByCodeFailByNonExistence() throws DAOException {        
        CollaborativeProject collaborativeProject;               
        collaborativeProject = COLLABORATIVE_PROJECT_DAO.getCollaborativeProjectByCode(COLLABORATIVE_PROJECT_FOR_TESTING.getCode());
        assertEquals(0,collaborativeProject.getIdCollaborativeProject());
    }
    
    @Test
    public void getCollaborativeProjectByIdCollaborativeProjectSucces() throws DAOException {
        setUp();
        CollaborativeProject collaborativeProject;
        int idCollaborativeProject;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);        
        collaborativeProject = COLLABORATIVE_PROJECT_DAO.getCollaborativeProjectByIdCollaborativeProject(idCollaborativeProject);        
        assertEquals(COLLABORATIVE_PROJECT_FOR_TESTING,collaborativeProject);
    }
    
    @Test 
    public void getCollaborativeProjectByIdCollaborativeProjectFailByNonExistence() throws DAOException {        
        CollaborativeProject collaborativeProject;               
        collaborativeProject = COLLABORATIVE_PROJECT_DAO.getCollaborativeProjectByIdCollaborativeProject(COLLABORATIVE_PROJECT_FOR_TESTING.getIdCollaborativeProject());
        assertEquals(0,collaborativeProject.getIdCollaborativeProject());
    }
    
    @Test
    public void updatePendingCollaborativeProjectSucces() throws DAOException {
        setUp();
        int idCollaborativeProject;
        int result = 0;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_FOR_TESTING.setName("Bases de datos y Programación");
        result = COLLABORATIVE_PROJECT_DAO.updateCollaborativeProject(COLLABORATIVE_PROJECT_FOR_TESTING);
        assertTrue(result > 0);
    }
    
    @Test
    public void updateRejectedCollaborativeProjectSucces() throws DAOException {
        setUp();
        int idCollaborativeProject;
        int result = 0;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(COLLABORATIVE_PROJECT_FOR_TESTING, "Rechazado");
        COLLABORATIVE_PROJECT_FOR_TESTING.setName("Bases de datos y Programación");
        result = COLLABORATIVE_PROJECT_DAO.updateCollaborativeProject(COLLABORATIVE_PROJECT_FOR_TESTING);
        assertTrue(result > 0);
    }
    
    @Test
    public void updateCollaborativeProjectFailByAlreadyAcceptedCollaborativeProjectProposal() throws DAOException {
        setUp();
        int idCollaborativeProject;
        int result = 0;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal
        (COLLABORATIVE_PROJECT_FOR_TESTING, "Aceptado");
        COLLABORATIVE_PROJECT_FOR_TESTING.setName("Bases de datos y Programación");
        DAOException exception = assertThrows
        (DAOException.class, () -> COLLABORATIVE_PROJECT_DAO.updateCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING));        
        System.out.println(exception.getMessage());
    }
    
    @Test
    public void updateCollaborativeProjectFailByAlreadyFinalizedCollaborativeProjectProposal() throws DAOException {
        testHelper.initializeCollaborativeProject();
        CollaborativeProject collaborativeProject;
        collaborativeProject = testHelper.getCollaborativeProject();
        int result = 0;                
        COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject(collaborativeProject);        
        collaborativeProject.setName("Bases de datos y Programación");
        DAOException exception = assertThrows
        (DAOException.class, () -> COLLABORATIVE_PROJECT_DAO.updateCollaborativeProject
        (collaborativeProject));        
        System.out.println(exception.getMessage());
    }
    
    @Test
    public void updateCollaborativeProjectFailByDuplicateCode() throws DAOException {
        testHelper.initializeCollaborativeProject();
        initializeCollaborativeProject();
        COLLABORATIVE_PROJECT_FOR_TESTING.setRequesterCourse(testHelper.getCourseThree());
        COLLABORATIVE_PROJECT_FOR_TESTING.setRequestedCourse(testHelper.getCourseFour());
        auxCollaborativeProjectRequest = testHelper.getCollaborativeProjectRequestTwo();
        int idCollaborativeProject;                        
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_FOR_TESTING.setCode(testHelper.getCollaborativeProject().getCode());
        DAOException exception = assertThrows
        (DAOException.class, () -> COLLABORATIVE_PROJECT_DAO.updateCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING));        
        System.out.println(exception.getMessage());                
    }
    
    @Test
    public void finalizeCollaborativeProjectSucces() throws DAOException {
        setUp();
        int idCollaborativeProject;
        int result = 0;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(COLLABORATIVE_PROJECT_FOR_TESTING, "Aceptado");
        result = COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject(COLLABORATIVE_PROJECT_FOR_TESTING);
        assertTrue(result > 0);
    }
    
    @Test
    public void finalizeCollaborativeProjectFailByInappropiateState() throws DAOException {
        setUp();
        int idCollaborativeProject;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        DAOException exception = assertThrows
        (DAOException.class, () -> COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING));        
        System.out.println(exception.getMessage());
    }
    
    @Test
    public void hasThreeActivitiesAtLeastSucces() throws DAOException {
        boolean check;
        testHelper.initializeAssignments();
        CollaborativeProject collaborativeProject;
        collaborativeProject = testHelper.getCollaborativeProject();
        check = COLLABORATIVE_PROJECT_DAO.hasThreeActivitiesAtLeast(collaborativeProject);
        assertTrue(check);
    }
    
    @Test
    public void hasThreeActivitiesAtLeastFail() throws DAOException {
        setUp();
        int idCollaborativeProject;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        DAOException exception = assertThrows
        (DAOException.class, () -> COLLABORATIVE_PROJECT_DAO.hasThreeActivitiesAtLeast
        (COLLABORATIVE_PROJECT_FOR_TESTING));
        System.out.println(exception.getMessage());
    }
            
    @Test
    public void getCollaborativeProjectsProposalsSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        result = COLLABORATIVE_PROJECT_DAO.getCollaborativeProjectsProposals();
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
    public void getCollaborativeProjectsProposalsFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        result = COLLABORATIVE_PROJECT_DAO.getCollaborativeProjectsProposals();
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getAllAcceptedCollaborativeProjectsSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Aceptado");
        }
        result = COLLABORATIVE_PROJECT_DAO.getAllAcceptedCollaborativeProjects();
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
    public void getAllAcceptedCollaborativeProjectsFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        result = COLLABORATIVE_PROJECT_DAO.getAllAcceptedCollaborativeProjects();
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getAllRejectedCollaborativeProjectsSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Rechazado");
        }
        result = COLLABORATIVE_PROJECT_DAO.getAllRejectedCollaborativeProjects();
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
    public void getAllRejectedCollaborativeProjectsFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        result = COLLABORATIVE_PROJECT_DAO.getAllRejectedCollaborativeProjects();
        assertTrue(result.isEmpty());
    }
        
    @Test
    public void getAllFinishedCollaborativeProjectsSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();     
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Aceptado");
            COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject(collaborativeProject);
        }
        result = COLLABORATIVE_PROJECT_DAO.getAllFinishedCollaborativeProjects();
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
    public void getAllFinishedCollaborativeProjectsFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        result = COLLABORATIVE_PROJECT_DAO.getAllFinishedCollaborativeProjects();
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getPendingCollaborativeProjectsByProfessorOneSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        Professor professor;
        professor = testHelper.getProfessorOne();
        result = COLLABORATIVE_PROJECT_DAO.getPendingCollaborativeProjectsByProfessor(professor.getIdProfessor());
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
    public void getPendingCollaborativeProjectsByProfessorOneFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        testHelper.initializeProfessors();
        Professor professor;
        professor = testHelper.getProfessorOne();
        result = COLLABORATIVE_PROJECT_DAO.getPendingCollaborativeProjectsByProfessor(professor.getIdProfessor());
        assertTrue(result.isEmpty());
    }        
    
    @Test
    public void getPendingCollaborativeProjectsByProfessorTwoSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        Professor professor;
        professor = testHelper.getProfessorTwo();
        result = COLLABORATIVE_PROJECT_DAO.getPendingCollaborativeProjectsByProfessor(professor.getIdProfessor());
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
    public void getPendingCollaborativeProjectsByProfessorTwoFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        testHelper.initializeProfessors();
        Professor professor;
        professor = testHelper.getProfessorTwo();
        result = COLLABORATIVE_PROJECT_DAO.getPendingCollaborativeProjectsByProfessor(professor.getIdProfessor());
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getPendingCollaborativeProjectsByProfessorFailByNonExistenceIdProfessor() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        result = COLLABORATIVE_PROJECT_DAO.getPendingCollaborativeProjectsByProfessor(0);
        deleteCollaborativeProjects();
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getAcceptedCollaborativeProjectsByProfessorOneSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        Professor professor;
        professor = testHelper.getProfessorOne();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Aceptado");
        }
        result = COLLABORATIVE_PROJECT_DAO.getAcceptedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
    public void getAcceptedCollaborativeProjectsByProfessorOneFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        testHelper.initializeProfessors();
        Professor professor;
        professor = testHelper.getProfessorOne();
        result = COLLABORATIVE_PROJECT_DAO.getAcceptedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getAcceptedCollaborativeProjectsByProfessorTwoSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        Professor professor;
        professor = testHelper.getProfessorTwo();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Aceptado");
        }
        result = COLLABORATIVE_PROJECT_DAO.getAcceptedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
    public void getAcceptedCollaborativeProjectsByProfessorTwoFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        testHelper.initializeProfessors();
        Professor professor;
        professor = testHelper.getProfessorTwo();
        result = COLLABORATIVE_PROJECT_DAO.getAcceptedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getAcceptedCollaborativeProjectsByProfessorFailByNonExistenceIdProfessor() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Aceptado");
        }
        result = COLLABORATIVE_PROJECT_DAO.getAcceptedCollaborativeProjectsByProfessor(0);
        deleteCollaborativeProjects();
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getRejectedCollaborativeProjectsByProfessorOneSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        Professor professor;
        professor = testHelper.getProfessorOne();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Rechazado");
        }
        result = COLLABORATIVE_PROJECT_DAO.getRejectedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
    public void getRejectedCollaborativeProjectsByProfessorOneFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        testHelper.initializeProfessors();
        Professor professor;
        professor = testHelper.getProfessorOne();
        result = COLLABORATIVE_PROJECT_DAO.getRejectedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getRejectedCollaborativeProjectsByProfessorTwoSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        Professor professor;
        professor = testHelper.getProfessorTwo();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Rechazado");
        }
        result = COLLABORATIVE_PROJECT_DAO.getRejectedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);
    }
    
    @Test
     public void getRejectedCollaborativeProjectsByProfessorTwoFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        testHelper.initializeProfessors();
        Professor professor;
        professor = testHelper.getProfessorTwo();
        result = COLLABORATIVE_PROJECT_DAO.getRejectedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getRejectedCollaborativeProjectsByProfessorFailByNonExistenceIdProfessor() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Rechazado");
        }
        result = COLLABORATIVE_PROJECT_DAO.getRejectedCollaborativeProjectsByProfessor(0);
        deleteCollaborativeProjects();
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getFinishedCollaborativeProjectsByProfessorOneSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        Professor professor;
        professor = testHelper.getProfessorOne();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Aceptado");
            COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject(collaborativeProject);
        }
        result = COLLABORATIVE_PROJECT_DAO.getFinishedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);        
    }
    
    @Test
    public void getFinishedCollaborativeProjectsByProfessorOneFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        testHelper.initializeProfessors();
        Professor professor;
        professor = testHelper.getProfessorOne();
        result = COLLABORATIVE_PROJECT_DAO.getFinishedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getFinishedCollaborativeProjectsByProfessorTwoSucces() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        Professor professor;
        professor = testHelper.getProfessorTwo();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Aceptado");
            COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject(collaborativeProject);
        }
        result = COLLABORATIVE_PROJECT_DAO.getFinishedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        deleteCollaborativeProjects();
        assertEquals(COLLABORATIVE_PROJECTS_FOR_TESTING,result);        
    }
    
    @Test
    public void getFinishedCollaborativeProjectsByProfessorTwoFail() throws DAOException {
        ArrayList<CollaborativeProject> result;
        testHelper.initializeProfessors();
        Professor professor;
        professor = testHelper.getProfessorTwo();
        result = COLLABORATIVE_PROJECT_DAO.getFinishedCollaborativeProjectsByProfessor(professor.getIdProfessor());
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getFinishedCollaborativeProjectsByProfessorFailByNonExistenceIdProfessor() throws DAOException {
        ArrayList<CollaborativeProject> result;
        initializeCollaborativeProjects();
        for (CollaborativeProject collaborativeProject : COLLABORATIVE_PROJECTS_FOR_TESTING) {
            COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(collaborativeProject,"Aceptado");
            COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject(collaborativeProject);
        }
        result = COLLABORATIVE_PROJECT_DAO.getFinishedCollaborativeProjectsByProfessor(0);
        deleteCollaborativeProjects();
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void getFinishedCollaborativeProjectByIdRequesterCourseSucces() throws DAOException {
        setUp();
        CollaborativeProject collaborativeProject;
        Course curse;
        curse = COLLABORATIVE_PROJECT_FOR_TESTING.getRequesterCourse();
        int idCollaborativeProject;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(COLLABORATIVE_PROJECT_FOR_TESTING,"Aceptado");
        COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject(COLLABORATIVE_PROJECT_FOR_TESTING);        
        collaborativeProject = COLLABORATIVE_PROJECT_DAO.getFinishedCollaborativeProjectByIdCourse(curse.getIdCourse());        
        assertEquals(COLLABORATIVE_PROJECT_FOR_TESTING,collaborativeProject);
    }
    
    @Test
    public void getFinishedCollaborativeProjectByIdRequestedCourseSucces() throws DAOException {
        setUp();
        CollaborativeProject collaborativeProject;
        Course curse;
        curse = COLLABORATIVE_PROJECT_FOR_TESTING.getRequestedCourse();
        int idCollaborativeProject;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(COLLABORATIVE_PROJECT_FOR_TESTING,"Aceptado");
        COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject(COLLABORATIVE_PROJECT_FOR_TESTING);        
        collaborativeProject = COLLABORATIVE_PROJECT_DAO.getFinishedCollaborativeProjectByIdCourse(curse.getIdCourse());        
        assertEquals(COLLABORATIVE_PROJECT_FOR_TESTING,collaborativeProject);
    }
    
    @Test
    public void getFinishedCollaborativeProjectByIdCourseFailByNonExistenceIdCourse() throws DAOException {               
        setUp();
        CollaborativeProject collaborativeProject;
        int idCollaborativeProject;
        idCollaborativeProject = COLLABORATIVE_PROJECT_DAO.registerCollaborativeProject
        (COLLABORATIVE_PROJECT_FOR_TESTING, auxCollaborativeProjectRequest);
        COLLABORATIVE_PROJECT_FOR_TESTING.setIdCollaborativeProject(idCollaborativeProject);
        COLLABORATIVE_PROJECT_DAO.evaluateCollaborativeProjectProposal(COLLABORATIVE_PROJECT_FOR_TESTING,"Aceptado");
        COLLABORATIVE_PROJECT_DAO.finalizeCollaborativeProject(COLLABORATIVE_PROJECT_FOR_TESTING);        
        collaborativeProject = COLLABORATIVE_PROJECT_DAO.getFinishedCollaborativeProjectByIdCourse(0); 
        assertEquals(0,collaborativeProject.getIdCollaborativeProject());        
    }
            
    @After
    public void tearDown() {
        try {
            if (COLLABORATIVE_PROJECT_FOR_TESTING.getIdCollaborativeProject() > 0) {
                COLLABORATIVE_PROJECT_DAO.deleteCollaborativeProjectByidCollaborativeProject(COLLABORATIVE_PROJECT_FOR_TESTING.getIdCollaborativeProject());
            }
        } catch (DAOException exception) {
            Log.getLogger(CollaborativeProjectTest.class).error(exception.getMessage(), exception);
        }
        testHelper.deleteAll();
    }    
}