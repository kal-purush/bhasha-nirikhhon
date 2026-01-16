package Classes;

import Loaders.ApplicationLoader;
import Users.User.MaritalStatus;
import Utilities.Searchable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Application implements Searchable {
    private String projectName;
    private String applicantName;
    private String applicantNRIC;
    private int age;
    private MaritalStatus maritalStatus;
    private String flatType;
    private ApplicationStatus applicationStatus;

    public enum ApplicationStatus {
        PENDING,
        SUCCESSFUL,
        UNSUCCESSFUL,
        APPLY_FOR_BOOKING,
        BOOKED,
        IN_WITHDRAWAL,
        PENDING_IN_WITHDRAWAL,
        SUCCESSFUL_IN_WITHDRAWAL,
        APPLY_FOR_BOOKING_IN_WITHDRAWAL,
        BOOKED_IN_WITHDRAWAL,
        //SUCCESS_WITHDRAWAL,
        //REJECT_WITHDRAWAL,
        PROJECT_DELETED
    }

    public Application(String projectName, String applicantName, String applicantNRIC, int age, MaritalStatus maritalStatus, String flatType, ApplicationStatus status) {
        this.projectName = projectName;
        this.applicantName = applicantName;
        this.applicantNRIC = applicantNRIC;
        this.age = age;
        this.maritalStatus = maritalStatus;
        this.flatType = flatType;
        this.applicationStatus = status;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getApplicantName() {
        return applicantName;
    }

    public void setApplicantName(String applicantName) {
        this.applicantName = applicantName;
    }

    public String getApplicantNRIC() {
        return applicantNRIC;
    }

    public void setApplicantNRIC(String applicantNRIC) {
        this.applicantNRIC = applicantNRIC;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public MaritalStatus getMaritalStatus() {
        return maritalStatus;
    }

    public void setMaritalStatus(MaritalStatus maritalStatus) {
        this.maritalStatus = maritalStatus;
    }

    public String getFlatType() {
        return flatType;
    }

    public void setFlatType(String flatType) {
        this.flatType = flatType;
    }

    public ApplicationStatus getApplicationStatus() {
        return applicationStatus;
    }

    public void setApplicationStatus(ApplicationStatus applicationStatus) {
        this.applicationStatus = applicationStatus;
    }

    public String IDstring() {return applicantNRIC;};

    public String defaultString(){
        return (projectName+applicantName+applicantNRIC);
    };

    public String toSearchableString() {
        return (
                projectName
                        + " " + applicantName
                        + " " + maritalStatus
                        + " " + flatType
                        + " " + applicationStatus
        ).toLowerCase();
    }

    public List<Integer> toSearchableNum() {
        return new ArrayList<>(List.of(age));
    }

    // Filter applications by project name
    public static ArrayList<Application> getApplicationsList(String projectName) {
        ArrayList<Application> filteredList = new ArrayList<>();
        for (Application application : ApplicationLoader.loadApplications()) {
            if (application.projectName.equals(projectName)) {
                filteredList.add(application);
            }
        }
        return filteredList;
    }



    @Override
    public String toString() {
        String leftColFormat = "%-20s %s";
        StringBuilder sb = new StringBuilder();

        sb.append(String.format(leftColFormat, "Project Name:", projectName)).append("\n");
        sb.append(String.format(leftColFormat, "Applicant Name:", applicantName)).append("\n");
        sb.append(String.format(leftColFormat, "NRIC:", applicantNRIC)).append("\n");
        sb.append(String.format(leftColFormat, "Age:", age)).append("\n");
        sb.append(String.format(leftColFormat, "Marital Status:", maritalStatus)).append("\n");
        sb.append(String.format(leftColFormat, "Flat Type:", flatType)).append("\n");
        sb.append(String.format(leftColFormat, "Application Status:", applicationStatus));

        return sb.toString();
    }

}