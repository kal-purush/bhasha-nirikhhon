package studentcourse;

public class Course {
	public Course() {
		super();
	}


	public Course(int courseId, String courseName, String instructorName, int maxStudents, int currentStudents,
			String schedule) {
		super();
		this.courseId = courseId;
		this.courseName = courseName;
		this.instructorName = instructorName;
		this.maxStudents = maxStudents;
		this.currentStudents = currentStudents;
		this.schedule = schedule;
	}


	public int getCourseId() {
		return courseId;
	}


	public void setCourseId(int courseId) {
		this.courseId = courseId;
	}


	public String getCourseName() {
		return courseName;
	}


	public void setCourseName(String courseName) {
		this.courseName = courseName;
	}


	public String getInstructorName() {
		return instructorName;
	}


	public void setInstructorName(String instructorName) {
		this.instructorName = instructorName;
	}


	public int getMaxStudents() {
		return maxStudents;
	}


	public void setMaxStudents(int maxStudents) {
		this.maxStudents = maxStudents;
	}


	public int getCurrentStudents() {
		return currentStudents;
	}


	public void setCurrentStudents(int currentStudents) {
		this.currentStudents = currentStudents;
	}


	public String getSchedule() {
		return schedule;
	}


	public void setSchedule(String schedule) {
		this.schedule = schedule;
	}


	private int courseId;
    private String courseName;
    private String instructorName;
    private int maxStudents;
    private int currentStudents; 
    private String schedule;
    
    @Override
    public String toString() {
        return "Course ID: " + courseId +
                ", Course Name: " + courseName +
                ", Instructor Name: " + instructorName +
                ", Max Students: " + maxStudents;
    }
    
    
    public void displayCourseDetails() {
        System.out.println("Course Details:");
        System.out.println("  Course ID: " + courseId);
        System.out.println("  Course Name: " + courseName);
        System.out.println("  Instructor Name: " + instructorName);
        System.out.println("  Max Students: " + maxStudents);
    }
}