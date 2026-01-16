package it.pagopa.pm.gateway.dto.xpay;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigInteger;

@Data
@AllArgsConstructor
public class XpayPersistableRequest {
    private String codiceTransazione;
    private BigInteger importo;
    private String divisa;
    private String timeStamp;
}