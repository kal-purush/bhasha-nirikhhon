package com.metamong.mt.domain.member.dto.response;

import java.time.LocalDate;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;


@Getter
@Builder
@AllArgsConstructor
public class MemberResponseDto {
	private final String userId;
	private final String name;
	private final String email;
	private final String phone;
	private final LocalDate birth;
	private final String postalCode;
	private final String detailAddress;
    private final String address;
    private final String businessName;
    private final String businessRegistrationNumber;
}