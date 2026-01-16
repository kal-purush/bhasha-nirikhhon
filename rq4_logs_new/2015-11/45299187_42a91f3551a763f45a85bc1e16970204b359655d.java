package beans;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;

import model.Lecturer;
import persistanceService.LecturerPrefDAO;

@ManagedBean(name="lecturerPrefBean")
@RequestScoped
public class LecturerPrefBean{

	private Lecturer lecturer;
	private List<String> prefNumbers;
	private StudentBean studentBean;
	private LecturerPrefDAO lecturerPrefDAO;

	public LecturerPrefBean(){
		lecturer = new Lecturer();
		prefNumbers = new ArrayList<String>();
	}
	
	public final Lecturer getLecturer() {
		return lecturer;
	}

	public final void setLecturer(Lecturer lecturer) {
		this.lecturer = lecturer;
	}

	public final List<String> getPrefNumbers() {
		return prefNumbers;
	}

	public final void setPrefNumbers(List<String> prefNumbers) {
		this.prefNumbers = prefNumbers;
	}

	public final StudentBean getStudentBean() {
		return studentBean;
	}

	public final void setStudentBean(StudentBean studentBean) {
		this.studentBean = studentBean;
	}

	public void savePreferences(){
		lecturerPrefDAO = new LecturerPrefDAO();
		try {
			lecturerPrefDAO.savePreferences(lecturer, prefNumbers);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}