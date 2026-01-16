package unit.test.Initializer;

import log.Log;
import mx.fei.coilvicapp.logic.collaborativeproject.CollaborativeProject;
import mx.fei.coilvicapp.logic.collaborativeproject.CollaborativeProjectDAO;
import mx.fei.coilvicapp.logic.collaborativeprojectrequest.CollaborativeProjectRequest;
import mx.fei.coilvicapp.logic.collaborativeprojectrequest.CollaborativeProjectRequestDAO;
import mx.fei.coilvicapp.logic.country.Country;
import mx.fei.coilvicapp.logic.country.CountryDAO;
import mx.fei.coilvicapp.logic.course.Course;
import mx.fei.coilvicapp.logic.course.CourseDAO;
import mx.fei.coilvicapp.logic.implementations.DAOException;
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
public class TestHelper {

    private Language language;
    private Term term;
    private Modality modality;
    private Country countryOne;
    private Country countryTwo;
    private University universityOne;
    private University universityTwo;
    private Professor professorOne;
    private Professor professorTwo;
    private Course courseOne;
    private Course courseTwo;
    private CollaborativeProjectRequest collaborativeProjectRequest;
    private CollaborativeProject collaborativeProject;

    public TestHelper() {
        language = new Language();
        term = new Term();
        modality = new Modality();
        countryOne = new Country();
        countryTwo = new Country();
        universityOne = new University();
        universityTwo = new University();
        professorOne = new Professor();
        professorTwo = new Professor();
        courseOne = new Course();
        courseTwo = new Course();
        collaborativeProjectRequest = new CollaborativeProjectRequest();
        collaborativeProject = new CollaborativeProject();

    }

    public void initializeLanguage() {
        LanguageDAO languageDAO = new LanguageDAO();
        language.setName("Inglés");

        try {
            language.setIdLanguage(languageDAO.registerLanguage(language));

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
    }

    public void initializeTerm() {
        TermDAO termDAO = new TermDAO();
        term.setName("Febrero2024-Junio2024");

        try {
            term.setIdTerm(termDAO.registerTerm(term));

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
    }

    public void initializeModality() {
        ModalityDAO modalityDAO = new ModalityDAO();
        modality.setName("Clase espejo");

        try {
            modality.setIdModality(modalityDAO.registerModality(modality));

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
    }

    public void initializeCountries() {
        CountryDAO countryDAO = new CountryDAO();
        countryOne = new Country();
        countryTwo = new Country();
        countryOne.setName("México");
        countryTwo.setName("Venezuela");

        try {
            countryOne.setIdCountry(countryDAO.registerCountry(countryOne));
            countryTwo.setIdCountry(countryDAO.registerCountry(countryTwo));

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
    }

    public void initializeUniversities() {
        initializeCountries();
        UniversityDAO universityDAO = new UniversityDAO();
        universityOne.setName("Universidad Veracruzana");
        universityOne.setAcronym("UV");
        universityOne.setJurisdiction("Veracruz");
        universityOne.setCity("Xalapa");
        universityOne.setCountry(countryOne);

        universityTwo.setName("Universidad Católica Andrés Bello");
        universityTwo.setAcronym("UCAB");
        universityTwo.setJurisdiction("Caracas");
        universityTwo.setCity("Guayana");
        universityTwo.setCountry(countryTwo);
        try {
            universityOne.setIdUniversity(universityDAO.registerUniversity(universityOne));
            universityTwo.setIdUniversity(universityDAO.registerUniversity(universityTwo));

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
    }

    public void initializeProfessors() {
        initializeUniversities();
        ProfessorDAO professorDAO = new ProfessorDAO();
        professorOne.setName("Maria");
        professorOne.setPaternalSurname("Arenas");
        professorOne.setMaternalSurname("Valdes");
        professorOne.setEmail("aaren@uv.mx");
        professorOne.setGender("Mujer");
        professorOne.setPhoneNumber("1234567890");
        professorOne.setUniversity(universityOne);

        professorTwo.setName("Jorge");
        professorTwo.setPaternalSurname("Gonzalez");
        professorTwo.setMaternalSurname("Hernandez");
        professorTwo.setEmail("joGo@gmail.com");
        professorTwo.setGender("Hombre");
        professorTwo.setPhoneNumber("1234567890");
        professorTwo.setUniversity(universityTwo);
        try {
            professorOne.setIdProfessor(professorDAO.registerProfessor(professorOne));
            professorTwo.setIdProfessor(professorDAO.registerProfessor(professorTwo));

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
    }

    public void intializeCourses() {
        initializeProfessors();
        initializeTerm();
        initializeLanguage();
        CourseDAO courseDAO = new CourseDAO();
        courseOne.setName("Programacion");
        courseOne.setGeneralObjective("Los alumnos comprendan la"
                + " programacion estructurada y orientada a objetos");
        courseOne.setTopicsInterest("Abstraccion, Herencia, Polimorfismo");
        courseOne.setNumberStudents(35);
        courseOne.setStudentsProfile("Ingenieria de software");
        courseOne.setTerm(term);
        courseOne.setLanguage(language);
        courseOne.setAdditionalInformation("Ademas de la programacion"
                + " orientada a objetos veremos el paradigma funcional");
        courseOne.setProfessor(professorOne);

        courseTwo.setName("Bases de Datos");
        courseTwo.setGeneralObjective("Los alumnos comprendan el diseño, "
                + "implementación y administración de bases de datos relacionales.");
        courseTwo.setTopicsInterest("Modelado de datos, SQL, Normalización");
        courseTwo.setNumberStudents(40);
        courseTwo.setStudentsProfile("Ingeniería en Sistemas Computacionales");
        courseTwo.setTerm(term);
        courseTwo.setLanguage(language);
        courseTwo.setAdditionalInformation("Además de bases de datos relacionales, "
                + "se introducirá a bases de datos NoSQL y técnicas avanzadas de optimización.");
        courseTwo.setProfessor(professorTwo);
        try {
            courseTwo.setIdCourse(courseDAO.registerCourse(courseTwo));
            courseOne.setIdCourse(courseDAO.registerCourse(courseOne));
            courseDAO.evaluateCourseProposal(courseTwo, "Aceptado");
            courseDAO.evaluateCourseProposal(courseOne, "Aceptado");

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
        courseTwo.setStatus("Aceptado");
        courseOne.setStatus("Aceptado");
    }

    public void initializeCollaborativeProjectRequest() {
        intializeCourses();
        CollaborativeProjectRequestDAO collaborativeProjectRequestDAO = new CollaborativeProjectRequestDAO();
        collaborativeProjectRequest.setRequestedCourse(courseOne);
        collaborativeProjectRequest.setRequesterCourse(courseTwo);

        try {
            collaborativeProjectRequest.setIdCollaborativeProjectRequest(
                    collaborativeProjectRequestDAO.registerCollaborativeProjectRequest(
                            collaborativeProjectRequest));
            collaborativeProjectRequestDAO.attendCollaborativeProjectRequest(collaborativeProjectRequest, "Aceptado");

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
        collaborativeProjectRequest.setStatus("Aceptado");
    }

    public void initializeCollaborativeProject() {
        initializeCollaborativeProjectRequest();
        initializeModality();
        CollaborativeProjectDAO collaborativeProjectDAO = new CollaborativeProjectDAO();
        collaborativeProject.setName("Programación y Bases de Datos");
        collaborativeProject.setStatus("Aceptado");
        collaborativeProject.setDescription("Este proyecto combina los conocimientos de programación orientada a objetos y bases de datos "
                + "para desarrollar una aplicación completa que gestione información de manera eficiente.");
        collaborativeProject.setGeneralObjective("Integrar conceptos de programación y bases de datos para desarrollar una solución software completa.");
        collaborativeProject.setModality(modality);
        collaborativeProject.setCode("ProgBasdat2024!");
        collaborativeProject.setSyllabusPath("/syllabus/proyecto_integrador.pdf");
        collaborativeProject.setRequestedCourse(courseOne);
        collaborativeProject.setRequesterCourse(courseTwo);
        try {
            collaborativeProject.setIdCollaborativeProject(collaborativeProjectDAO.registerCollaborativeProject(collaborativeProject, collaborativeProjectRequest));
            collaborativeProjectDAO.evaluateCollaborativeProjectProposal(collaborativeProject, "Aceptado");

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
        collaborativeProject.setStatus("Aceptado");
    }

    public void deleteAll() {
        try {
            new CollaborativeProjectDAO().deleteCollaborativeProjectByidCollaborativeProject(collaborativeProject.getIdCollaborativeProject());
            new CollaborativeProjectRequestDAO().deleteCollaborativeProjectRequestByidCollaborativeProjectRequest(collaborativeProjectRequest.getIdCollaborativeProjectRequest());
            new ModalityDAO().deleteModality(modality.getIdModality());
            new CourseDAO().deleteCourseByIdCourse(courseOne.getIdCourse());
            new CourseDAO().deleteCourseByIdCourse(courseTwo.getIdCourse());
            new LanguageDAO().deleteLanguage(language.getIdLanguage());
            new TermDAO().deleteTerm(term.getIdTerm());
            new ProfessorDAO().deleteProfessorByID(professorOne.getIdProfessor());
            new ProfessorDAO().deleteProfessorByID(professorTwo.getIdProfessor());
            new UniversityDAO().deleteUniversity(universityOne.getIdUniversity());
            new UniversityDAO().deleteUniversity(universityTwo.getIdUniversity());
            new CountryDAO().deleteCountry(countryOne.getIdCountry());
            new CountryDAO().deleteCountry(countryTwo.getIdCountry());

        } catch (DAOException exception) {
            Log.getLogger(AssignmentRegistrationTest.class
            ).error(exception.getMessage(), exception);
        }
    }

    public Language getLanguage() {
        return language;
    }

    public void setLanguage(Language language) {
        this.language = language;
    }

    public Term getTerm() {
        return term;
    }

    public void setTerm(Term term) {
        this.term = term;
    }

    public Modality getModality() {
        return modality;
    }

    public void setModality(Modality modality) {
        this.modality = modality;
    }

    public Country getCountryOne() {
        return countryOne;
    }

    public void setCountryOne(Country countryOne) {
        this.countryOne = countryOne;
    }

    public Country getCountryTwo() {
        return countryTwo;
    }

    public void setCountryTwo(Country countryTwo) {
        this.countryTwo = countryTwo;
    }

    public University getUniversityOne() {
        return universityOne;
    }

    public void setUniversityOne(University universityOne) {
        this.universityOne = universityOne;
    }

    public University getUniversityTwo() {
        return universityTwo;
    }

    public void setUniversityTwo(University universityTwo) {
        this.universityTwo = universityTwo;
    }

    public Professor getProfessorOne() {
        return professorOne;
    }

    public void setProfessorOne(Professor professorOne) {
        this.professorOne = professorOne;
    }

    public Professor getProfessorTwo() {
        return professorTwo;
    }

    public void setProfessorTwo(Professor professorTwo) {
        this.professorTwo = professorTwo;
    }

    public Course getCourseOne() {
        return courseOne;
    }

    public void setCourseOne(Course courseOne) {
        this.courseOne = courseOne;
    }

    public Course getCourseTwo() {
        return courseTwo;
    }

    public void setCourseTwo(Course courseTwo) {
        this.courseTwo = courseTwo;
    }

    public CollaborativeProjectRequest getCollaborativeProjectRequest() {
        return collaborativeProjectRequest;
    }

    public void setCollaborativeProjectRequest(CollaborativeProjectRequest collaborativeProjectRequest) {
        this.collaborativeProjectRequest = collaborativeProjectRequest;
    }

    public CollaborativeProject getCollaborativeProject() {
        return collaborativeProject;
    }

    public void setCollaborativeProject(CollaborativeProject collaborativeProject) {
        this.collaborativeProject = collaborativeProject;
    }

}