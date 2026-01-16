package spike;

public class Course {

	private String courseNumber;
	private Section section;
	private String courseName;
	private double creditHours;

	public String getCourseNumber() {
		return courseNumber;
	}

	public void setCourseNumber(String courseNumber) {
		this.courseNumber = courseNumber;
	}

	public Section getSection() {
		return section;
	}

	public void setSection(Section sections) {
		this.section = sections;
	}

	public String getCourseName() {
		return courseName;
	}

	public void setCourseName(String courseName) {
		this.courseName = courseName;
	}

	public double getCreditHours() {
		return creditHours;
	}

	public void setCreditHours(double creditHours) {
		this.creditHours = creditHours;
	}

}