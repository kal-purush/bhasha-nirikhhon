package dao;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jdbc.ProveedorDeConeccion;
import tierraMedia.Atraccion;
import tierraMedia.Posicion;
import tierraMedia.Producto;
import tierraMedia.PromoPorcentual;
import tierraMedia.PromocionAxB;
import tierraMedia.TipoAtraccion;
import tierraMedia.Usuario;
;

public class DataBaseTest {

	private void inicializarBD() throws SQLException {
//		ProveedorDeConeccion.closeConnection();
		ProveedorDeConeccion.getConnection();
		
		ProveedorDeConeccion.insertarDatosDePruebaEnLaBD();
	}
	
	@Before
	public void db() throws SQLException {
		inicializarBD();
	}
	
	@After
	public void cerrarDB() throws SQLException {
		inicializarBD();
	}
	
	@Test
	public void queAlConsultarUnUsuarioDevuelvaTodosSusDatos() {
		UsuarioDAO usuarioDAO = DAOFactory.getUsuarioDAO();

		assertTrue(usuarioDAO.buscarPorIdUsuario(1).getNombre().equals("EowyN"));
		assertTrue(usuarioDAO.buscarPorIdUsuario(1).getPreferencia().equals(TipoAtraccion.AVENTURA));
		assertEquals(90, usuarioDAO.buscarPorIdUsuario(1).getPresupuesto());
		assertEquals(1, usuarioDAO.buscarPorIdUsuario(1).getPosX(), 0);
		assertEquals(2, usuarioDAO.buscarPorIdUsuario(1).getPosY(), 0);
		
	}
	
	
	@Test
	public void queAlConsultarUnaAtraccionDevuelvaTodosSusDatos() {
		AtraccionDAO atraccionDAO = DAOFactory.getAtraccionDAO();

		assertTrue(atraccionDAO.buscarPorIdAtraccion((long)1).getNombre().equals("Moria"));
		assertTrue(atraccionDAO.buscarPorIdAtraccion((long)1).getTipo().equals(TipoAtraccion.AVENTURA));
		assertEquals(10, atraccionDAO.buscarPorIdAtraccion((long)1).getCosto());
		assertEquals(46, atraccionDAO.buscarPorIdAtraccion((long)1).getCupo());
		assertEquals(2.0, atraccionDAO.buscarPorIdAtraccion((long)1).getDuracion(), 0);
		
	}
	
	@Test
	public void queAlConsultarUnaPromocionDevuelvaTodosSusDatos() {
		PromocionDAO promocionDAO = DAOFactory.getPromocionDAO();
		
		assertTrue(promocionDAO.buscarPorIdPromocion((long)1).getNombre().equals("Pack aventura"));
		
		assertTrue(promocionDAO.buscarPorIdPromocion((long)1).getTipo().equals(TipoAtraccion.AVENTURA));
		
		assertEquals(22, promocionDAO.buscarPorIdPromocion((long)1).getCosto());
		
		assertEquals(5, promocionDAO.buscarPorIdPromocion((long)1).getDuracion(), 0);
		
		assertEquals(2, promocionDAO.buscarPorIdPromocion((long)1).getAtraccionesIncluidas().size());
		assertTrue(promocionDAO.buscarPorIdPromocion((long)1).getAtraccionesIncluidas().get(0).getNombre().equals("Moria"));
		assertTrue(promocionDAO.buscarPorIdPromocion((long)1).getAtraccionesIncluidas().get(1).getNombre().equals("Mordor"));

	}
	
		
	@Test
	public void queAlAceptarUnaSugerenciaSeCargueEnItinerarioDB() {
		UsuarioDAO usuarioDAO = DAOFactory.getUsuarioDAO();
		AtraccionDAO atraccionDAO = DAOFactory.getAtraccionDAO();
		ItinerarioDAO itinerarioDAO = DAOFactory.getItinerarioDAO();
		
		
		Usuario user1 = usuarioDAO.buscarPorIdUsuario(1);
		Atraccion atracc = atraccionDAO.buscarPorIdAtraccion((long) 1);
		
		user1.aceptarSugerencia(atracc);
		assertEquals(1, itinerarioDAO.itinerarioDelUsuarioPorId((long)1).size());
//		assertEquals(1, itinerarioDAO.itinerarioeHistorialDelUsuario(user1).getItinerario().size());
		assertTrue(itinerarioDAO.itinerarioeHistorialDelUsuario(user1).getItinerario().contains(atracc));

	}
	
	
	@Test
	public void queNoSeCarguenAtraccionesRepetidasEnDB() {
		UsuarioDAO usuarioDAO = DAOFactory.getUsuarioDAO();
		AtraccionDAO atraccionDAO = DAOFactory.getAtraccionDAO();
		ItinerarioDAO itinerarioDAO = DAOFactory.getItinerarioDAO();
		
		
		Usuario user1 = usuarioDAO.buscarPorIdUsuario(1);
		Atraccion atracc = atraccionDAO.buscarPorIdAtraccion((long) 1);
		
		user1.aceptarSugerencia(atracc);
		user1.aceptarSugerencia(atracc);
		assertEquals(1, itinerarioDAO.itinerarioDelUsuarioPorId((long)1).size());
//		assertEquals(1, itinerarioDAO.itinerarioeHistorialDelUsuario(user1).getItinerario().size());
		assertTrue(itinerarioDAO.itinerarioeHistorialDelUsuario(user1).getItinerario().contains(atracc));

	}
	
	@Test
	public void queAlAceptarUnaSugerenciaSeCargueEnItinerarioDB3() {
		UsuarioDAO usuarioDAO = DAOFactory.getUsuarioDAO();
		AtraccionDAO atraccionDAO = DAOFactory.getAtraccionDAO();
		ItinerarioDAO itinerarioDAO = DAOFactory.getItinerarioDAO();
		
		
		Usuario user1 = usuarioDAO.buscarPorIdUsuario(1);
		Atraccion atracc = atraccionDAO.buscarPorIdAtraccion((long) 1);
		Atraccion atracc2 = atraccionDAO.buscarPorIdAtraccion((long) 2);
		

		user1.aceptarSugerencia(atracc);
		assertEquals(1, itinerarioDAO.itinerarioDelUsuarioPorId((long)1).size());
		assertTrue(itinerarioDAO.itinerarioeHistorialDelUsuario(user1).getItinerario().contains(atracc));
		
		user1.aceptarSugerencia(atracc2);
		assertEquals(2, itinerarioDAO.itinerarioDelUsuarioPorId((long)1).size());
		assertTrue(itinerarioDAO.itinerarioeHistorialDelUsuario(user1).getItinerario().contains(atracc2));
	}
	
	@Test
	public void queAlAceptarUnaSugerenciaSeActualizanElDineroYElTiempoDisponibleDelUsuario() {
		UsuarioDAO usuarioDAO = DAOFactory.getUsuarioDAO();
		AtraccionDAO atraccionDAO = DAOFactory.getAtraccionDAO();
		
		
		Usuario user1 = usuarioDAO.buscarPorIdUsuario(1);
		Atraccion atracc = atraccionDAO.buscarPorIdAtraccion((long) 1);
		

		user1.aceptarSugerencia(atracc);
		assertEquals(80, usuarioDAO.buscarPorIdUsuario(1).getPresupuesto());
		assertEquals(88.0, usuarioDAO.buscarPorIdUsuario(1).getTiempoDisponible(), 0);
	}
	
	@Test
	public void queAlAceptarUnaAtraccionSeRestaEn1ElCupoDeLaAtraccion() {
		UsuarioDAO usuarioDAO = DAOFactory.getUsuarioDAO();
		AtraccionDAO atraccionDAO = DAOFactory.getAtraccionDAO();
		
		Usuario user1 = usuarioDAO.buscarPorIdUsuario(1);
		Atraccion atracc = atraccionDAO.buscarPorIdAtraccion((long) 1);
		

		user1.aceptarSugerencia(atracc);
		assertEquals(45, atracc.getCupo());
	}
	
	@Test
	public void queAlAceptarUnaPromocionSeRestaEn1ElCupoDeLasAtraccionesIncluidas() {
		UsuarioDAO usuarioDAO = DAOFactory.getUsuarioDAO();
		PromocionDAO promocionDAO = DAOFactory.getPromocionDAO();
		
		Usuario user1 = usuarioDAO.buscarPorIdUsuario(1);
		Producto promo = promocionDAO.buscarPorIdPromocion((long) 1);
		

		user1.aceptarSugerencia(promo);
		assertEquals(45, promo.getAtraccionesIncluidas().get(0).getCupo());
		assertEquals(95, promo.getAtraccionesIncluidas().get(1).getCupo());
	}
	
	@Test
	public void queAlAceptarUnaPromocionNoAceptaAtraccionesIndividualesConetenidasEnLaPromocionAceptada() {
		UsuarioDAO usuarioDAO = DAOFactory.getUsuarioDAO();
		AtraccionDAO atraccionDAO = DAOFactory.getAtraccionDAO();
		PromocionDAO promocionDAO = DAOFactory.getPromocionDAO();
		ItinerarioDAO itinerarioDAO = DAOFactory.getItinerarioDAO();
		
		Usuario user1 = usuarioDAO.buscarPorIdUsuario(1);
		Producto promo = promocionDAO.buscarPorIdPromocion((long) 1);
		Atraccion atracc = atraccionDAO.buscarPorIdAtraccion((long) 1);
		

		user1.aceptarSugerencia(promo);
		assertEquals(45, promo.getAtraccionesIncluidas().get(0).getCupo());
		assertEquals(95, promo.getAtraccionesIncluidas().get(1).getCupo());
		assertEquals(1, itinerarioDAO.itinerarioDelUsuarioPorId((long)1).size());
		
		user1.aceptarSugerencia(atracc);
		assertEquals(1, itinerarioDAO.itinerarioDelUsuarioPorId((long)1).size());
		
	}
}