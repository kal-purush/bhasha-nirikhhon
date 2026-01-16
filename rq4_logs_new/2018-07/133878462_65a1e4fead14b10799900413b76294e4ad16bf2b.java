package base.controle;

import java.io.Serializable;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;

import base.modelo.Usuario;


public class UsuarioController implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private Usuario usuario;
	public UsuarioController(){
		usuario = new Usuario();
		SecurityContext context = SecurityContextHolder.getContext();
		if(context instanceof SecurityContext)
		{
			Authentication authentication = context.getAuthentication();
			if(authentication instanceof Authentication)
			{
				 usuario.setLogin(((User)authentication.getPrincipal()).getUsername());
			}
		}
	}
	public Usuario getUsuario() {
		return usuario;
	}
	public void setUsuario(Usuario usuario) {
		this.usuario = usuario;
	}
}