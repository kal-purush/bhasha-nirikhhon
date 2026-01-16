package kr.itaz.lms.portal.vo;

import groovy.transform.ToString;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ToString
public class ServiceFareVO {
	private String svcId;
	private String fareSeCode;
	private String fareSeCodeNm;
	private String fareUnit;
	private Integer yearAccount;
	private Integer monthAccount;
}