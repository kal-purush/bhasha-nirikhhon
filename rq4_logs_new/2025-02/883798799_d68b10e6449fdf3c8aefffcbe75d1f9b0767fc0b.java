package it.gov.pagopa.payhub.pdnd.anpr.connector.c003;

import it.gov.pagopa.payhub.anpr.C003.dto.generated.RispostaE002OK;

public interface AnprC003Service {
    RispostaE002OK getUserData(String idAnpr, String fiscalCode);
}
