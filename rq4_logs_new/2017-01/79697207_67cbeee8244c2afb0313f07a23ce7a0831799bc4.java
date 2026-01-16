public class GradeBook {
	
	private String courseName;
	
	
	public void displayMessage() {	
		System.out.printf("Welcome to the %s!\n", courseName);
	}
	
	public void setCourseName(String name) {
		courseName = name;
	}

	public String getCourseName() {
		return courseName;
	}

}