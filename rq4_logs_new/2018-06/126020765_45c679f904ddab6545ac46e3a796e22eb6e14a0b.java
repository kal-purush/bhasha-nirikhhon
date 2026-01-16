package es.ucm.fdi.iw.common.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;
import org.springframework.ui.Model;

import es.ucm.fdi.iw.common.enums.Temas;
import es.ucm.fdi.iw.model.ComentarioForo;
import es.ucm.fdi.iw.model.Item;
import es.ucm.fdi.iw.model.User;

public final class Utils {
	
	public static final String mensaje = "mensaje";
	public static final String foro = "foro";
	public static final String tienda = "tienda";
	public static final String imagen = "imagen";
	public static final String user = "user";
	public static final String tema = "tema";
	public static final String saloon = "saloon";
	public static final String chatSocket = "chatsocket";
	public static final String partidaSocket = "partidasocket";
	public static final String perfil = "perfil";
	private static final String endpoint = "endpoint";
	
	@SuppressWarnings("unchecked")
	public static void foro(HttpSession session,EntityManager entityManager, Temas tema) {
		List<ComentarioForo> comentarios;
		comentarios = (List<ComentarioForo>)entityManager.createNamedQuery("getForo")
			           .setParameter("temaParam", tema)
			           .getResultList();
		Collections.sort(comentarios, new Comparator<ComentarioForo>() {
			public int compare(ComentarioForo c1, ComentarioForo c2) {
				return c1.getFecha().compareTo(c2.getFecha());
			}
		});
		session.setAttribute(foro,comentarios);
	}
	
	@SuppressWarnings("unchecked")
	public static void tienda(HttpSession session,EntityManager entityManager) {

		List<Item> tnd = (List<Item>)entityManager.createNamedQuery("getTienda")
			                 .getResultList();
		session.setAttribute(tienda, tnd);
	}
		
	public static void socket(Model model,HttpServletRequest request,String replace, String by) {
		model.addAttribute(endpoint, request.getRequestURL().toString()
				.replaceFirst("[^:]*", "ws")
				.replace(replace, by));
	}
	
	/**
	 * Actualiza y devuelve el usuario de sesión
	 * @param em: Punto de entrada a la base de datos
	 * @param session: Sesion del usuario
	 * @return Devuelve el usuario ya odificado
	 */
	public static User userFromSession(EntityManager em, HttpSession session) {
		User u = (User)session.getAttribute(user);
		u = em.find(User.class, u.getId());
		session.setAttribute(user, u);
		return u;
	}
	
	public static void eliminaVariablesSession(HttpSession session) {
		session.removeAttribute(Utils.user);
		session.removeAttribute(Utils.tema);
		session.removeAttribute(Utils.imagen);
		session.removeAttribute(Utils.mensaje);
		session.removeAttribute(Utils.foro);
		session.removeAttribute(Utils.tienda);
	}
	
	/**
	 * Operación auxiliar: Revisa si un usuario existe en la bd.
	 * 
	 * @param nombre:
	 *            Nombre del usuario a buscar.
	 * @return Devuelve el objeto si existe y null en caso contratio.
	 */	
	public static User usuarioExiste(EntityManager entityManager,String nombre,HttpSession session,Logger log) {
		
		User u = null;
		
		try {
			u = (User) entityManager.createNamedQuery("getUsuario").setParameter("loginParam", nombre)
					.getSingleResult();
		}catch (NoResultException e) {
			log.error(e);
			session.setAttribute(Utils.mensaje, nombre + " no existe.");
		}
		
		return u;
	}
	
	/**
	 * Operación auxiliar: Busca si un usuario tiene un item en concreto. 
	 * Si el parametro elimina está a true, además lo elimina.
	 * 
	 * @param i:
	 *            Item a buscar.
	 * @param elimina:
	 *            Modo eliminacion.          
	 * @param u:
	 *            Usuario en cuestión.         
	 * @return true si lo encontro, false en caso contrario.
	 */
	public static boolean usuarioTieneItem(Item i, boolean elimina, User u) {

		int iPropiedades = 0;

		while (iPropiedades < u.getPropiedades().size() && 
			   !u.getPropiedades().get(iPropiedades).getNombre()
			   .equals(i.getNombre())) {
			iPropiedades++;
		}

		if (iPropiedades != u.getPropiedades().size()) {

			int iPropietarios = 0;
			while (iPropietarios < i.getPropietarios().size() && 
				   i.getPropietarios().get(iPropiedades).getLogin().equals(u.getLogin())) {
				iPropietarios++;
			}

			if (elimina) {
				u.getPropiedades().remove(iPropiedades);
				i.getPropietarios().remove(iPropietarios);
			}

			return true;
		}

		return false;

	}

	public static void borraUsuario(User u,EntityManager entityManager) {
		u.setAmigos(null);
		
		int indiceCm = 0;
		ComentarioForo c;
		while(indiceCm < u.getComentarios().size()) {
			c = u.getComentarios().get(indiceCm);
			entityManager.remove(c);
			indiceCm++;
		}
		u.setComentarios(null);
		
		int indiceIt = 0;
		while(indiceIt < u.getPropiedades().size()) {
			usuarioTieneItem(u.getPropiedades().get(indiceIt), true, u); 
			indiceIt++;
		}
		
		u.setPropiedades(null);
		entityManager.remove(u);
	}

}