package com.gcp.recruitRight.Controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.gcp.recruitRight.Impls.PostRequirementImpl;
import com.gcp.recruitRight.Requests.PostRequirementRequest;
import com.gcp.recruitRight.response.BaseResponse;

@RestController
@CrossOrigin(origins="http://localhost:3000")
public class PostRequirementController {
	
	@Autowired
	PostRequirementImpl postRequirementImpl;
	
	@PostMapping("/postRequirement")
	public ResponseEntity<BaseResponse> postRequirement(@RequestBody PostRequirementRequest postRequirementRequest){	
		BaseResponse baseResponse = new BaseResponse();
		try {
			baseResponse.setBooleanMsg(postRequirementImpl.postRequirement(postRequirementRequest));
		} catch (Exception e) {
			baseResponse.setExceptionMessage(e.getMessage());
			baseResponse.setBooleanMsg(false);
		}
		return ResponseEntity.status(HttpStatus.CREATED).body(baseResponse);
	}

}