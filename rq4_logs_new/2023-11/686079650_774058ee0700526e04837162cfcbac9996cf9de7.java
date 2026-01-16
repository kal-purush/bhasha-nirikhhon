package by.bsuir.poit.servlets.command.impl;

import by.bsuir.poit.bean.Lot;
import by.bsuir.poit.bean.User;
import by.bsuir.poit.context.RequestHandlerDefinition;
import by.bsuir.poit.services.LotService;
import by.bsuir.poit.services.UserService;
import by.bsuir.poit.servlets.UserDetails;
import by.bsuir.poit.servlets.UserPageType;
import by.bsuir.poit.servlets.command.RequestHandler;
import by.bsuir.poit.utils.PageUtils;
import by.bsuir.poit.utils.Paginator;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * @author Paval Shlyk
 * @since 22/11/2023
 */
@RequiredArgsConstructor
@RequestHandlerDefinition(urlPatterns = "/auction/assign")
public class AuctionLotAssignment implements RequestHandler {
private static final Logger LOGGER = LogManager.getLogger(AdminHandler.class);
public static final String PAGE_TYPE = "pageType";
public static final String PAGE_LOTS = "lots";
public static final String USERNAME = "username";

private final LotService lotService;
private final UserService userService;

@Override
public void accept(HttpServletRequest request, HttpServletResponse response) throws Exception {
      UserDetails details = (UserDetails) request.getUserPrincipal();
      if (details.role() != User.ADMIN) {
	    response.sendError(HttpServletResponse.SC_FORBIDDEN, "You cannot see such page");
	    return;
      }
      long adminId = details.id();
      Paginator paginator = new Paginator(request, 5);
      List<Lot> uncommittedLots = lotService.findAllByStatus(Lot.BEFORE_AUCTION_STATUS);
      paginator.configure(uncommittedLots, PAGE_LOTS);
      User user = userService.findUserByUserId(adminId);
      request.setAttribute(USERNAME, user.getName());
      request.setAttribute(PAGE_TYPE, UserPageType.ADMIN);
      PageUtils.includeWith(request, response, PageUtils.AUCTION_ASSIGNMENT_PAGE);
}
}