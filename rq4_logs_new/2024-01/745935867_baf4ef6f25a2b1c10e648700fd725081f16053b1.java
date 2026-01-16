package com.springboot.GraphQL.response;

import com.springboot.GraphQL.entity.MemberType;

import lombok.Data;

@Data
public class MemberResponse {
	
	private int id;
	   
	private String name;
	   
	private String contact;
	
	private MemberType type;

}