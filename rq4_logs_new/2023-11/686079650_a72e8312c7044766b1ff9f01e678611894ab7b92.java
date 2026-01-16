package by.bsuir.poit.servlets.command.impl;

import by.bsuir.poit.context.RequestHandlerDefinition;
import by.bsuir.poit.services.LotService;
import by.bsuir.poit.servlets.command.RequestHandler;
import by.bsuir.poit.utils.ParserUtils;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * @author Paval Shlyk
 * @since 21/11/2023
 */
@RequestHandlerDefinition(urlPatterns = "/log/delete")
@RequiredArgsConstructor
public class LotDeletionHandler implements RequestHandler {
private static final Logger LOGGER = LogManager.getLogger(LotDeletionHandler.class);
private final LotService lotService;
@Override
public void accept(HttpServletRequest request, HttpServletResponse response) throws Exception {
      Optional<Long> optionalLotId = ParserUtils.parseRequest(Long.class, request, ParserUtils.LOT_ID);
      if (optionalLotId.isEmpty()) {
	    LOGGER.info("Invalid lot id to start lot deletion procedure");
	    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No lot id is specified");
	    return;
      }
      long lotId = optionalLotId.get();
      boolean isDeleted = lotService.deleteIfPossible(lotId);//when method throws ResourceBusyException, we cannot do anything
      if (!isDeleted) {
	    response.sendError(HttpServletResponse.SC_FORBIDDEN);
      } else {
	    response.setStatus(HttpServletResponse.SC_OK);
      }
}
}