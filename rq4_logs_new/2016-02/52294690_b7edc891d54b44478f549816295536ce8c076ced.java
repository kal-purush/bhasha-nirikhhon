package uo.sdi.acciones;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alb.util.log.Log;
import uo.sdi.acciones.Accion;
import uo.sdi.model.Seat;
import uo.sdi.model.SeatStatus;
import uo.sdi.model.Trip;
import uo.sdi.model.User;
import uo.sdi.persistence.PersistenceFactory;
import uo.sdi.persistence.SeatDao;
import uo.sdi.persistence.TripDao;

public class ApuntarseViajeAction implements Accion {

	@Override
	public String execute(HttpServletRequest request,
			HttpServletResponse response) {
		TripDao dao = PersistenceFactory.newTripDao();
		Trip viaje = dao.findById(Long.valueOf( request.getParameter("ID")));
		try {
		User usuario = (User) request.getSession().getAttribute("user");
		SeatDao daoS = PersistenceFactory.newSeatDao();
		Seat dto = new Seat();
		dto.setUserId(usuario.getId());
		dto.setTripId(viaje.getId());
		dto.setStatus(SeatStatus.PENDANT);
		ArrayList<Trip> lista = (ArrayList<Trip>) request.getSession().getAttribute("listaApuntado");
		daoS.save(dto);
		lista.add(PersistenceFactory.newTripDao().findById(dto.getTripId()));
		request.getSession().setAttribute("listaApuntado", lista);
		Log.debug("Reservada plaza para el viaje [%s] por el usuario [%s]",viaje.getId(), usuario.getLogin());
		}
		catch(Exception e){
			Log.error("Algo ha ocurrido reservando la plaza para el viaje [%s]",viaje.getId());
		}
		return "EXITO";
	}

}