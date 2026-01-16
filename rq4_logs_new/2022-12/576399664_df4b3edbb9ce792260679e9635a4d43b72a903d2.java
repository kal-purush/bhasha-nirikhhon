package com.mernis.wsdlclient.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mernis.wsdlclient.tr.gov.nvi.tckimlik.ws.*;

import java.net.MalformedURLException;
import java.net.URL;


@RestController
public class MernisController {
	
	@GetMapping
	public boolean mernisDogrula() throws MalformedURLException {
		String endpoint = "https://tckimlik.nvi.gov.tr/Service/KPSPublic.asmx?WSDL";
		URL url = new URL(endpoint);
		KPSPublic kpsPublic = new KPSPublic(url);
		KPSPublicSoap soap = kpsPublic.getKPSPublicSoap();
		 boolean tc = soap.tcKimlikNoDogrula(11111111111L, "Mehmet", "Basrioğlu", 1111);
		System.out.println("Gerçek kişi mi ? : "+tc);
		//yorum satırı
		return tc;
	}


}