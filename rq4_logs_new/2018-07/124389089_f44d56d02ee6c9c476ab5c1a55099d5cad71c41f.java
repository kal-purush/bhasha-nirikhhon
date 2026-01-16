package edu.ctb.upm.midas.client_modules.extraction.mayoclinic.texts_extraction.api_response;

import edu.ctb.upm.midas.model.extraction.common.request.RequestJSON;
import edu.ctb.upm.midas.model.extraction.common.response.Response;
import edu.ctb.upm.midas.model.extraction.common.request.Request;

/**
 * Created by gerardo on 12/02/2018.
 *
 * @author Gerardo Lagunes G. ${EMAIL}
 * @version ${<VERSION>}
 * @project dgsvp-rest
 * @className MayoClinicTextsExtractionService
 * @see
 */
public interface MayoClinicTextsExtractionService {

    Response getTexts(Request request);

    Response getResources(Request request);

    Response getTextsByJSON(RequestJSON request);

    Response getResourcesByJSON(RequestJSON request);
}