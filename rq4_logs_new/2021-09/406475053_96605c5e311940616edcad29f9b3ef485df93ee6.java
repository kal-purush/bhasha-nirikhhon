package ca.bc.gov.open.Scss.Configuration;

import java.util.ArrayList;
import java.util.List;
import javax.xml.transform.TransformerException;
import lombok.SneakyThrows;
import org.springframework.ws.context.MessageContext;
import org.springframework.ws.soap.server.endpoint.interceptor.PayloadValidatingInterceptor;
import org.xml.sax.SAXParseException;

public class CustomPayloadInterceptor extends PayloadValidatingInterceptor {

    @SneakyThrows
    @Override
    protected boolean handleRequestValidationErrors(
            MessageContext messageContext, SAXParseException[] errors) throws TransformerException {

        //        Remove date tie errors as date time parsing happens after this point
        List<SAXParseException> newErrors = new ArrayList<>();
        for (var error : errors) {
            if (!error.getLocalizedMessage().contains("dateTime")
                    && !error.getLocalizedMessage().contains("CourtAppearanceDate")) {
                newErrors.add(error);
            }
        }
        if (newErrors.size() == 0) {
            return true;
        }
        return super.handleRequestValidationErrors(
                messageContext, newErrors.toArray(new SAXParseException[0]));
    }
}