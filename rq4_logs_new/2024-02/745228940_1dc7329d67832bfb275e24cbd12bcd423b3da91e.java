public class ProjectDTO {
    Integer projectID;
    String projectName;
    String slug;

    public Integer getProjectID() {
        return projectID;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getSlug() {
        return slug;
    }

    public void setProjectID(Integer projectID) {
        this.projectID = projectID;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public ProjectDTO() {
    }

    public ProjectDTO(Integer projectID, String projectName, String slug) {
        this.projectID = projectID;
        this.projectName = projectName;
        this.slug = slug;
    }

    @Override
    public String toString() {
        return "projectID=" + projectID +
                ", projectName='" + projectName + '\'' +
                ", slug='" + slug + '\'';
   }
}