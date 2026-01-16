package by.bsuir.poit.servlets.command.impl;

import by.bsuir.poit.bean.Lot;
import by.bsuir.poit.context.RequestHandlerDefinition;
import by.bsuir.poit.services.LotService;
import by.bsuir.poit.servlets.command.RequestHandler;
import by.bsuir.poit.utils.ParserUtils;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * In contract to <code>LotPublishingHandler</code> this handle is used to all other requests, which are not supported by mentioned.
 * There are all kinds of modifications
 *
 * @author Paval Shlyk
 * @see LotPublishingHandler
 * @since 08/11/2023
 */
@RequestHandlerDefinition(urlPatterns = "/lot/modify")
@RequiredArgsConstructor
public class LotModificationHandler implements RequestHandler {
private final LotService lotService;
public static final String LOT_MODIFICATION_PARAMETER = "type";


public enum LotModificationType {
      AUCTION, CUSTOMER, DELIVERY_POINT
}

@Override
public void accept(HttpServletRequest request, HttpServletResponse response) throws Exception {
      Lot lot = ParserUtils.parse(Lot.class, request);
//      String modificationType = request.getParameter(LOT_MODIFICATION_PARAMETER);
//      Optional<LotModificationType> type = ParserUtils.parseEnum(LotModificationType.class, modificationType);
//      if (type.isEmpty()) {
//	    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid modification type");
//	    return;
//      }
      lotService.update(lot);
}
}