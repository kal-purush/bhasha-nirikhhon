package it.portfolio.hr.humanResource.seeding;

import it.portfolio.hr.humanResource.entities.*;
import it.portfolio.hr.humanResource.models.Enums.ControctsEnum;
import it.portfolio.hr.humanResource.models.Enums.PerTimeEnum;
import it.portfolio.hr.humanResource.models.Enums.RoleEnum;
import it.portfolio.hr.humanResource.repositories.ApplicantRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

@Component
public class SeedingProvider {


    public List<Applicants> generateApplicant() {
        List<Applicants> applicantsList = new ArrayList<>();
        applicantsList.add(new Applicants("Giovanni", "Lo Russo", "LRSGNN80A01G273N", "ForeachSolutions", false));
        applicantsList.add(new Applicants("Salvo", "Regeni", "SLVRGN72S01G273E", "ForeachSolutions", false));
        applicantsList.add(new Applicants("Gianluca", "Regeni", "GNLRGN72S01G273D", "ForDevelopment s.r.l.", false));
        applicantsList.add(new Applicants("Baldo", "Rossi", "BLDRSS02C07G273L", "ForDevelopment s.r.l.", false));
        return applicantsList;
    }

    public List<CandidateEvaluations> generateCandidateEvaluations(List<Applicants> applicantsList) {
        List<CandidateEvaluations> candidateEvaluationsList = new ArrayList<>();
        for(Applicants applicants: applicantsList) {
            candidateEvaluationsList.add(new CandidateEvaluations(applicants, "E' andata davvero benissimo", true,applicants.getCompanyName(), false));
        }
        return candidateEvaluationsList;
    }

    public List<Department> generateDepartment() {
        List<Department> departmentList = new ArrayList<>();
        departmentList.add(new Department("Informatica", "Dipartimento di Informatica", "ForeachSolutions", false));
        departmentList.add(new Department("Storia", "Dipartimento di Storia", "ForeachSolutions", false));
        departmentList.add(new Department("Scienze", "Dipartimento di Scienze", "ForDevelopment s.r.l.", false));
        return departmentList;
    }

    public List<Employees> generateEmployees() {
        List<Employees> employeesList = new ArrayList<>();
        employeesList.add(new Employees("Giovanni Lo Russo", "Corso dei Mille, 8", "LRSGNN80A01G273N", "25/06/2024", "ForeachSolutions", false));
        employeesList.add(new Employees("Baldo Rossi", "Via Rimembranza, 328", "BLDRSS02C07G273L", "17/06/2024", "ForeachSolutions", false));
        return employeesList;
    }

    public List<Hiring> generateHiring(List<Employees> employeesList, List<Department> departmentList) {
        List<Hiring> hiringList = new ArrayList<>();
        for(Employees employees: employeesList) {
            hiringList.add(new Hiring(90.0F, departmentList.get(1), employees.getHiringDate(), ControctsEnum.FIXED_TERM, PerTimeEnum.FULL_TIME, "GB33BUKB20201555555555", RoleEnum.EMPLOYEES, employees, "ForeachSolutions", false));
        }
        return hiringList;
    }

    public List<Interview> generateInterview(List<Applicants> applicantsList) {
        List<Interview> interviewList = new ArrayList<>();
        for(Applicants applicants: applicantsList) {
            interviewList.add(new Interview(applicants, LocalDate.now().toString(), LocalTime.now().toString(), applicants.getCompanyName(), false));
        }
        return interviewList;
    }

    public List<JobAnnouncement> generateJobAnnouncement(List<Department> departmentList) {
        List<JobAnnouncement> jobAnnouncementList = new ArrayList<>();
        for(Department department: departmentList) {
            jobAnnouncementList.add(new JobAnnouncement(ControctsEnum.FIXED_TERM, "Buuuuuu", PerTimeEnum.PART_TIME, "Cerchiamo", department, "ForeachSolutions", false));
        }
        return jobAnnouncementList;
    }

    public List<Overtime> generateOvertime (List<Employees> employeesList) {
        List<Overtime> overtimeList = new ArrayList<>();
        for(Employees employees: employeesList) {
            overtimeList.add(new Overtime(employees, 5, "ForeachSolutions", false));
        }
        return overtimeList;
    }

    public List<Permits> generatePermits(List<Employees> employeesList) {
        List<Permits> permitsList = new ArrayList<>();
        for(Employees employees: employeesList) {
            permitsList.add(new Permits(employees, 5, "ForeachSolutions", false));
        }
        return permitsList;
    }

    public List<SickDays> generateSickDays(List<Employees> employeesList) {
        List<SickDays> sickDaysList = new ArrayList<>();
        for(Employees employees: employeesList) {
            sickDaysList.add(new SickDays(employees, 5, "ForeachSolutions", false));
        }
        return sickDaysList;
    }

    public List<Vacations> generateVacations(List<Employees> employeesList) {
        List<Vacations> vacationsList = new ArrayList<>();
        for(Employees employees: employeesList) {
            vacationsList.add(new Vacations(LocalDate.now().toString(), "12/08/2024", 5,  employees, "ForeachSolutions", false));
        }
        return vacationsList;
    }
}