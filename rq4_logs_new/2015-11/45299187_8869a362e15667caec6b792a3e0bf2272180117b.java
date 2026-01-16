package beans;

import java.io.IOException;
import java.sql.SQLException;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;
import javax.faces.context.FacesContext;

import model.User;
import persistanceService.UserDAO;

@ManagedBean(name = "loginBean")
@SessionScoped
public class LoginBean {

	private User user;
	private UserDAO userDAO;
		
	public LoginBean() {
		user = new User();
	}

	public void login() {
		userDAO = new UserDAO();
		try {
			if(userDAO.getUser(user.getUsername(), user.getPassword()) != null){
				BeanUtils.getSession().setAttribute("user", user);
				
				if( userDAO.getUser(user.getUsername(), user.getPassword()).getRole().equals("STUDENT") ) {
//					FacesContext.getCurrentInstance().getExternalContext().redirect("studentForm.xhtml");
					FacesContext.getCurrentInstance().getExternalContext().redirect("studentPreference.xhtml");
				} else if( userDAO.getUser(user.getUsername(), user.getPassword()).getRole().equals("LECTURER") ){
//					FacesContext.getCurrentInstance().getExternalContext().redirect("lecturerForm.xhtml");
					FacesContext.getCurrentInstance().getExternalContext().redirect("lecturerPreference.xhtml");
				}
				
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	
	public User getUser() {
		return user;
	}
	public void setUser(User user) {
		this.user = user;
	}
	
}