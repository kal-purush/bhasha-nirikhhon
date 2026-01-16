package com.mernis.wsdlclient.services.impl;

import com.mernis.wsdlclient.services.base.BaseMernis;
import com.mernis.wsdlclient.tr.gov.nvi.tckimlik.ws.KPSPublic;
import com.mernis.wsdlclient.tr.gov.nvi.tckimlik.ws.KPSPublicSoap;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
public class MernisImpl implements BaseMernis{
    @Override
    public Map<String, Object> validateMe(String tc, String ad, String soyAd, String dogumYili) {
        String endpoint = "https://tckimlik.nvi.gov.tr/Service/KPSPublic.asmx?WSDL";
        URL url = null;
        try {
            Map<String,Object> response = new HashMap<>();
            url = new URL(endpoint);
            KPSPublic kpsPublic = new KPSPublic(url);
            KPSPublicSoap soap = kpsPublic.getKPSPublicSoap();
            boolean result = soap.tcKimlikNoDogrula(11111111111L, "Mehmet", "Basrioğlu", 1111);
            response.put("endpoint",endpoint);
            response.put("isRealPersonInTurkey",result);
            response.put("status", HttpStatus.OK);
            return response;
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}