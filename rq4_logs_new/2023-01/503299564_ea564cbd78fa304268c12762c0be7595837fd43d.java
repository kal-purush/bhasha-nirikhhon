package it.pagopa.pn.client.b2b.pa.testclient;

import it.pagopa.pn.client.web.generated.openapi.clients.externalUserAttributes.io.model.IoCourtesyDigitalAddressActivation;
import org.springframework.web.client.RestClientException;

public interface IPnIoUserAttributerExternaClientImpl {

    IoCourtesyDigitalAddressActivation getCourtesyAddressIo(String xPagopaCxTaxid) throws RestClientException;

    void setCourtesyAddressIo(String xPagopaCxTaxid, IoCourtesyDigitalAddressActivation ioCourtesyDigitalAddressActivation) throws RestClientException;
}