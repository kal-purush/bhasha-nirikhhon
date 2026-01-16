package kyunysh;

import java.io.Serializable;
import java.util.Collection;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.inject.Named;

import kyunysh.dao.UsersDao;

@Named
@RequestScoped
public class ManagmentPageController implements Serializable {

	private static final long serialVersionUID = 1L;

	@Inject
	private UsersDao users;
	private User selectedUser = null;

	public ManagmentPageController() {
		// TODO Auto-generated constructor stub
	}

	public Collection<User> getUsers() {
		return users.getUsers();
	}

	public void removeUser() {
		users.removeUser(selectedUser);
	}

	public User getSelectedUser() {
		return selectedUser;
	}

	public void setSelectedUser(final User selectedUser) {
		this.selectedUser = selectedUser;
	}
}