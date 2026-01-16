package by.bsuir.poit.servlets.command.impl;

import by.bsuir.poit.bean.*;
import by.bsuir.poit.context.RequestHandlerDefinition;
import by.bsuir.poit.services.AuctionService;
import by.bsuir.poit.services.LotService;
import by.bsuir.poit.services.UserService;
import by.bsuir.poit.services.exception.authorization.UserNotFoundException;
import by.bsuir.poit.services.exception.resources.ResourceNotFoundException;
import by.bsuir.poit.servlets.command.RequestHandler;
import by.bsuir.poit.utils.PageUtils;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * This one handles request to basic info about auction (if it may be possible I'd inject <code>@PathVariable</code> but it's too hard to implement).
 * The current implementation should fetch id auction explicitly
 *
 * @author Paval Shlyk
 * @since 20/11/2023
 */

@RequestHandlerDefinition(urlPatterns = "/auction/info")
@RequiredArgsConstructor
public class AuctionInfoHandler implements RequestHandler {
private static final Logger LOGGER = LogManager.getLogger(AuctionInfoHandler.class);
public static final String AUCTION_ID_PARAMETER = "auction_id";
//the parameters for jsp page
//the common info about auction
public static final String AUCTION_TYPE_NAME = "auctionTypeName";
public static final String AUCTION_ID = "auctionId";
public static final String EVENT_DATE = "eventDate";
public static final String MEMBER_LIMIT = "memberLimit";
public static final String PRICE_STEP = "priceStep";
public static final String ADMIN_NAME = "adminName";
public static final String LOT_LIST = "lotList";
//auction specific info
public static final String BLIND_AUCTION_BET_LIMIT = "blindAuctionBetLimit";
public static final String BLIND_AUCTION_TIMEOUT = "blindAuctionTimeout";
public static final String BLITZ_AUCTION_EXCLUDE_COUNT = "blitzAuctionExcludeCount";
private final Map<Class<? extends Auction>, BiConsumer<HttpServletRequest, Auction>> requestSpecificAuctionHandlerMap = Map.of(
    BlitzAuction.class, this::putBlitzAuctionInfo,
    BlindAuction.class, this::putBlindAuctionInfo
);
private final AuctionService auctionService;
private final UserService userService;
private final LotService lotService;

@Override
public void accept(HttpServletRequest request, HttpServletResponse response) throws Exception {
      Optional<Long> optionalAuctionId = parseAuctionId(request);
      if (optionalAuctionId.isEmpty()) {
	    String message = "Invalid auction id in AuctionInfoHandler request";
	    LOGGER.info(message);
	    response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
	    return;
      }
      long auctionId = optionalAuctionId.get();
      try {
	    Auction auction = auctionService.findById(auctionId);
	    AuctionType type = auctionService.findTypeByAuctionId(auction.getId());
	    User admin = userService.findUserByUserId(auction.getAdminId());
	    if (admin.getRole() != User.ADMIN) {
		  final String msg = String.format("User that holds auction by id=%d admin position is not admin", auction.getId());
		  LOGGER.error(msg);
		  throw new IllegalStateException(msg);
	    }
	    List<Lot> lots = lotService.findAllByAuction(auction.getId());
	    request.setAttribute(AUCTION_TYPE_NAME, type.getName());
	    request.setAttribute(EVENT_DATE, auction.getLastRegisterDate());
	    if (auction.getMembersLimit() != null) {
		  request.setAttribute(MEMBER_LIMIT, auction.getMembersLimit());
	    }
	    request.setAttribute(PRICE_STEP, auction.getPriceStep());
	    request.setAttribute(ADMIN_NAME, admin.getName());
	    request.setAttribute(LOT_LIST, lots);
	    request.setAttribute(PageUtils.LOT_STATUSES, PageUtils.LOT_STATUSES_MAP);
	    request.setAttribute(AUCTION_ID, auction.getId());
	    BiConsumer<HttpServletRequest, Auction> handler = requestSpecificAuctionHandlerMap.get(auction.getClass());
	    if (handler != null) {
		  handler.accept(request, auction);
	    }
      } catch (ResourceNotFoundException e) {
	    String msg = String.format("The auction by given id=%s is not present", auctionId);
	    LOGGER.info(msg);
	    response.sendError(HttpServletResponse.SC_NOT_FOUND, msg);
      } catch (UserNotFoundException e) {
	    String msg = String.format("The admin for auction for given id=%d is not set", auctionId);
	    LOGGER.error(msg);
	    throw new IllegalStateException(msg);
      }
}

private void putBlitzAuctionInfo(HttpServletRequest request, Auction origin) {
      assert origin instanceof BlitzAuction;
      BlitzAuction auction = (BlitzAuction) origin;
      request.setAttribute(BLITZ_AUCTION_EXCLUDE_COUNT, auction.getMemberExcludeLimit());
}

private void putBlindAuctionInfo(HttpServletRequest request, Auction origin) {
      assert origin instanceof BlindAuction;
      BlindAuction auction = (BlindAuction) origin;
      request.setAttribute(BLIND_AUCTION_TIMEOUT, auction.getTimeout());
      request.setAttribute(BLIND_AUCTION_BET_LIMIT, auction.getBetLimit());
}

private Optional<Long> parseAuctionId(HttpServletRequest request) {
      Optional<Long> result = Optional.empty();
      try {
	    Long auctionId = Long.parseLong(request.getParameter(AUCTION_ID_PARAMETER));
	    result = Optional.of(auctionId);
      } catch (Exception ignored) {
      }
      return result;
}
}