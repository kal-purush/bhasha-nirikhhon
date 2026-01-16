package studentenrollment.SearchApi.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchEntity {
    private String studentName;
    private String username;

    @JsonProperty("studentId")
    private int id; // Field name as "id" in the class, displayed as "studentId" in JSON

    private String institutionName;
    private String entryLevel;

    public SearchEntity() {
    }

    public SearchEntity(String studentName, String username, int id, String institutionName, String entryLevel) {
        this.studentName = studentName;
        this.username = username;
        this.id = id;
        this.institutionName = institutionName;
        this.entryLevel = entryLevel;
    }

    // Getters and setters
    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getInstitutionName() {
        return institutionName;
    }

    public void setInstitutionName(String institutionName) {
        this.institutionName = institutionName;
    }

    public String getEntryLevel() {
        return entryLevel;
    }

    public void setEntryLevel(String entryLevel) {
        this.entryLevel = entryLevel;
    }
}