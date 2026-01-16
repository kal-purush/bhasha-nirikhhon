/**
 * 
 */
package com.fra.clientes.services.exceptions;

import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 * Clase utilitaria para manejar todo tipo de errores
 * 
 * @author renzo.ariel.felitti
 *
 */
public class ExceptionHandler {

	private static final Logger LOGGER = Logger.getLogger(ExceptionHandler.class);
	
	public static void handleException(Exception e) throws ServiceException {
		LOGGER.error(e.getMessage());
		if (e instanceof SQLException) {
			throw new ClienteNotFoundException(e.getMessage());
		} else {
			throw new InternalException(e.getMessage());
		}
	}
}