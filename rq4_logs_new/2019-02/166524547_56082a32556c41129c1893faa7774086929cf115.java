package com.hopper;

import com.hopper.constants.GlobalConstants;
import com.hopper.model.availability.AvailabilityRequest;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import scala.collection.immutable.Map;
import scala.util.parsing.json.JSONObject;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

public class AvailabilityProcessor
{
    public static Response process(final Request request, final String propertyID) throws JAXBException
    {
        final Response response = Response.apply(new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK));
        response.setContentTypeJson();
        response.setContentString(_getResponseJSON(propertyID, request));
        return response;
    }


    private static String _getResponseJSON(final String propertyID, final Request request) throws JAXBException
    {
        final AvailabilityRequest availabilityRequest = _createRequest(propertyID, request);

        JAXBContext context = JAXBContext.newInstance(AvailabilityRequest.class);
        Marshaller m = context.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        // Write to System.out
        m.marshal(availabilityRequest, System.out);

        final JSONObject jsonObject = new JSONObject(new Map.Map1("ID", propertyID));
        return jsonObject.toString();
    }

    private static AvailabilityRequest _createRequest(final String propertyID, final Request request)
    {
        final AvailabilityRequest availabilityRequest = new AvailabilityRequest();

        availabilityRequest.setCheckInDate(request.getParam(GlobalConstants.CHECKIN_PARAM_KEY));
        availabilityRequest.setCheckOutDate(request.getParam(GlobalConstants.CHECKOUT_PARAM_KEY));
        availabilityRequest.setCurrency(request.getParam(GlobalConstants.CURRENCY_CODE_KEY));
        availabilityRequest.setLanguage(request.getParam(GlobalConstants.LANGUAGE_CODE_KEY));

        availabilityRequest.setPropertyIds(propertyID);
        availabilityRequest.setApiKey("00000000-0000-0000-0000-000000000000");

        return availabilityRequest;
    }
}