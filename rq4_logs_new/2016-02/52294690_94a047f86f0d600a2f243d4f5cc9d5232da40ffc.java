package uo.sdi.acciones;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import uo.sdi.model.User;
import uo.sdi.model.UserStatus;
import uo.sdi.persistence.PersistenceFactory;
import uo.sdi.persistence.UserDao;
import alb.util.log.Log;

public class CrearUsuarioAccion implements Accion {

	@Override
	public String execute(HttpServletRequest request,
			HttpServletResponse response) {
		User dto = new User();
		dto.setLogin(request.getParameter("login"));
		dto.setName(request.getParameter("name"));
		dto.setSurname(request.getParameter("surname"));
		dto.setEmail(request.getParameter("email"));
		dto.setPassword(request.getParameter("password"));
		dto.setStatus(UserStatus.ACTIVE);
		String pass2 = request.getParameter("password2");
		try {
			if (dto.getPassword().equals(pass2)) {
				UserDao dao = PersistenceFactory.newUserDao();
				dao.save(dto);
				Log.debug("Creado el usuario [%s]",
						dto.getLogin());
			}
			else{
				return "FRACASO";
			}
		} catch (Exception e) {
			Log.error("Algo ha ocurrido creando el usuario [%s]",
					dto.getLogin());
		}
		return "EXITO";
	}

}