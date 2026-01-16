
public class Student {
	private String firstName;
	private String lastName;
	private double gpa;
	private String algGrade;
	private String bioGrade;
	private String engGrade;
	
	public Student(String firstName, String lastName, double gpa, String algGrade, String bioGrade, String engGrade)
	{
		this.firstName = firstName;
		this.lastName = lastName;
		this.gpa = gpa;
		this.algGrade = algGrade;
		this.bioGrade = bioGrade;
		this.engGrade = engGrade;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public double getGpa() {
		return gpa;
	}

	public void setGpa(double gpa) {
		this.gpa = gpa;
	}

	public String getAlgGrade() {
		return algGrade;
	}

	public void setAlgGrade(String algGrade) {
		this.algGrade = algGrade;
	}

	public String getBioGrade() {
		return bioGrade;
	}

	public void setBioGrade(String bioGrade) {
		this.bioGrade = bioGrade;
	}

	public String getEngGrade() {
		return engGrade;
	}

	public void setEngGrade(String engGrade) {
		this.engGrade = engGrade;
	}
	
	

}