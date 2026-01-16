package beans;

import java.sql.SQLException;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;

import model.Student;
import persistanceService.StudentDAO;

@ManagedBean(name="studentBean")
@RequestScoped
public class StudentBean{

	private Student student;
	private StudentDAO studentDAO;

	public StudentBean(){
		student = new Student(); //altfel ar fi null cand fac #{studentBean.student.name}
	}
	
	public Student getStudent() {
		return student;
	}

	public void setStudent(Student student) {
		this.student = student;
	}

	public void createStudent(){
		studentDAO = new StudentDAO();
		try {
			studentDAO.insertStudent(student);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}