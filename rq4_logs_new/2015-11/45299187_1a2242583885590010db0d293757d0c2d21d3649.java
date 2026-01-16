package beans;

import java.sql.SQLException;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;

import model.Lecturer;
import persistanceService.LecturerDAO;

@ManagedBean(name="lecturerBean")
@RequestScoped
public class LecturerBean{

	private Lecturer lecturer;
	private LecturerDAO lecturerDAO;

	public LecturerBean(){
		lecturer = new Lecturer();
	}
	
	public Lecturer getLecturer() {
		return lecturer;
	}

	public void setLecturer(Lecturer Lecturer) {
		this.lecturer = Lecturer;
	}

	public void createLecturer(){
		lecturerDAO = new LecturerDAO();
		try {
			lecturerDAO.insertLecturer(lecturer);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}