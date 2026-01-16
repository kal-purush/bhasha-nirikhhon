package by.bsuir.poit.servlets.command.impl;

import by.bsuir.poit.context.RequestHandlerDefinition;
import by.bsuir.poit.services.AuctionService;
import by.bsuir.poit.services.LotService;
import by.bsuir.poit.services.exception.authorization.UserAccessViolationException;
import by.bsuir.poit.servlets.command.RequestHandler;
import by.bsuir.poit.utils.ControllerUtils;
import by.bsuir.poit.utils.PageUtils;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Paval Shlyk
 * @since 23/11/2023
 */
@RequiredArgsConstructor
@RequestHandlerDefinition(urlPatterns = "/lot/assign")
public class LotAssignmentHandler implements RequestHandler {
private static final Logger LOGGER = LogManager.getLogger(LotAssignmentHandler.class);
public static final String LOT_ID_PARAMETER = "lot_id";
public static final String AUCTION_ID_PARAMETER = "auction_id";
private final AuctionService auctionService;

@Override
public void accept(HttpServletRequest request, HttpServletResponse response) throws Exception {
      long lotId;
      long auctionId;
      try {
	    lotId = Long.parseLong(request.getParameter(LOT_ID_PARAMETER));
	    auctionId = Long.parseLong(request.getParameter(AUCTION_ID_PARAMETER));
      } catch (NumberFormatException e) {
	    LOGGER.info("Failed to parse auction");
	    response.sendError(HttpServletResponse.SC_BAD_REQUEST);
	    return;
      }
      try {
	    auctionService.assignLot(request.getUserPrincipal(), auctionId, lotId);
      } catch (UserAccessViolationException e) {
	    LOGGER.info("Attempt to assign lot to auction by unauthorized user={}", request.getUserPrincipal());
	    response.sendError(HttpServletResponse.SC_FORBIDDEN);
	    return;
      }
      PageUtils.redirectTo(response, ControllerUtils.ASSIGNMENT_ENDPOINT + "?auction_id=" + auctionId);
}
}