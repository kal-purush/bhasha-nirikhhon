package com.lucamusic.order.controller.error;

/**
 * EventNotFoundException
 * Clase que trata el error 404: Event no encontrado
 * 
 * @author Jose Antonio
 * @version 1.0, Septiembre 1.0
 *
 */
public class EventNotFoundException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private static final String DESCRIPTION = "Not Found Exception (404)";
	
	public EventNotFoundException() {
		super(DESCRIPTION + ": No se encontró el evento");
	}
	
	public EventNotFoundException(Long id) {
		super(DESCRIPTION + ": No se encontró el evento " + id);
	}
}