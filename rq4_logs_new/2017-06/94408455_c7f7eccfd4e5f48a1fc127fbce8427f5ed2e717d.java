package com.ac.common.fabric;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.hyperledger.fabric.sdk.helper.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ac.common.fabric.config.HospitalInfoConfig;
import com.ac.common.fabric.config.InsuranceInfoConfig;
import com.ac.common.fabric.config.OrgCommonConfig;
import com.ac.common.fabric.hfc.OrgInfo;

import lombok.Getter;

@Service
public class OrgWapper {

	@Autowired
	private HospitalInfoConfig hospitalInfoConfig;

	@Autowired
	private InsuranceInfoConfig insuranceInfoConfig;

	@Getter
	private OrgInfo hospitalOrgInfo;

	@Getter
	private OrgInfo insuranceOrgInfo;

	@PostConstruct
	private void init() {

		hospitalOrgInfo = this.initOrgInfo(hospitalInfoConfig);
		insuranceOrgInfo = this.initOrgInfo(insuranceInfoConfig);

	}

	private OrgInfo initOrgInfo(OrgCommonConfig config) {

		hospitalOrgInfo = new OrgInfo();

		String[] ps = config.getPeerLocations().split("[ \t]*,[ \t]*");
		if (ArrayUtils.isEmpty(ps)) {
			throw new IllegalArgumentException("Peers is empty!");
		}

		for (String peer : ps) {
			String[] nl = peer.split("[ \t]*@[ \t]*");
			hospitalOrgInfo.addPeerLocation(nl[0], grpcTLSify(nl[1]));
		}

		hospitalOrgInfo.setDomainName(config.getDomname());

		ps = config.getOrdererLocations().split("[ \t]*,[ \t]*");
		if (ArrayUtils.isEmpty(ps)) {
			throw new IllegalArgumentException("Orderer is empty!");
		}

		for (String orderer : ps) {
			String[] nl = orderer.split("[ \t]*@[ \t]*");
			hospitalOrgInfo.addOrdererLocation(nl[0], grpcTLSify(nl[1]));
		}

		ps = config.getEventhubLocations().split("[ \t]*,[ \t]*");
		for (String event : ps) {
			String[] nl = event.split("[ \t]*@[ \t]*");
			hospitalOrgInfo.addEventHubLocation(nl[0], grpcTLSify(nl[1]));
		}

		hospitalOrgInfo.setCaLocation(httpTLSify(config.getCaLocation()));

		return hospitalOrgInfo;

	}

	private String grpcTLSify(String location) {

		location = StringUtils.trim(location);
		Exception e = Utils.checkGrpcUrl(location);
		if (e != null) {
			throw new RuntimeException(String.format("Bad TEST parameters for grpc url %s", location), e);
		}

		return location;
	}

	private String httpTLSify(String location) {
		return StringUtils.trim(location);
	}

}