package org.matgyeojo.controller;

import org.matgyeojo.dto.PetProfile;
import org.matgyeojo.dto.PetTendency;
import org.matgyeojo.repository.PetProfileRepo;
import org.matgyeojo.service.PetProfileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PetProfileController {
	@Autowired
	PetProfileService PetProfileService;
	
	//반려동물 등록 페이지 : 반려동물 정보 Pet profile 테이블에 저장
	@PostMapping("/petProfileSave")
	public void petprofileSave (@RequestBody PetProfile dto) {
		PetProfileService.petprofileSave(dto);
	}
	
	//반려동물 등록 페이지 : 반려동물 성향 Pet tendency 테이블에 저장
	@PostMapping("/petTendencySave")
	public void petTendencySave (@RequestBody PetTendency dto) {
		PetProfileService.petTendencySave(dto);
	}
}