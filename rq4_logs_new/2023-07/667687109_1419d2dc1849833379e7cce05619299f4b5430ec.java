/*
* AMRIT – Accessible Medical Records via Integrated Technology
* Integrated EHR (Electronic Health Records) Solution
*
* Copyright (C) "Piramal Swasthya Management and Research Institute"
*
* This file is part of AMRIT.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see https://www.gnu.org/licenses/.
*/
package com.iemr.helpline104.service.CTI;

import java.net.InetAddress;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.client.RestTemplate;

import com.google.gson.Gson;


@Service
public class CTIService {
    
	/*
	@Autowired
	private BeneficiaryRegistrationController beneficiaryRegistrationController; */
	
	private String domain="http://10.201.13.17";
	private String resFormat="1";
	private String ip="10.208.94.134";
	public String loginAgent(String agent_id) throws Exception {
		
		final String transaction_id="CTI_LOGIN";
		
		InetAddress inetAddress = InetAddress.getLocalHost();
		//String ip = inetAddress.getHostAddress();

		RestTemplate restTemplate = new RestTemplate();
		String url=domain+"/apps/appsHandler.php?transaction_id="+transaction_id+"&agent_id="+agent_id+"&ip="+ip+"&resFormat="+resFormat;
		
		ResponseEntity<String> res= restTemplate.getForEntity(url, String.class);
		
		return new Gson().toJson(res.getBody());
	}
	
	public String logoutAgent(String agent_id) throws Exception {
		
		final String transaction_id="CTI_LOGOUT";
		
		InetAddress inetAddress = InetAddress.getLocalHost();
		//String ip = inetAddress.getHostAddress();
		
		RestTemplate restTemplate = new RestTemplate();
		String url=domain+"/apps/appsHandler.php?transaction_id="+transaction_id+"&agent_id="+agent_id+"&ip="+ip+"&resFormat="+resFormat;
		
		ResponseEntity<String> res= restTemplate.getForEntity(url, String.class);
		
		return new Gson().toJson(res.getBody());
	}
	
	
	public String transferCall(String transfer_from,String transfer_to) throws Exception {
		
		final String transaction_id="CTI_TRANSFER_AGENT";
		
		InetAddress inetAddress = InetAddress.getLocalHost();
		//String ip = inetAddress.getHostAddress();
		
		RestTemplate restTemplate = new RestTemplate();
		String url=domain+"/apps/appsHandler.php?transaction_id="+transaction_id+"&transfer_ from="+transfer_from+"&transfer_to="+transfer_to+"&ip="+ip;
		
		ResponseEntity<String> res= restTemplate.getForEntity(url, String.class);
		
		return new Gson().toJson(res.getBody());
	}
	
}