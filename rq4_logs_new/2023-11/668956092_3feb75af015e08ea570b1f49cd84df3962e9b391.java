package com.oscarmartinez.socialleague.resource;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ReportHDCPDTO {
	
	private String category;
	private List<ReportInformationHDCP> players;

}