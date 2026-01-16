package com.gslab.talent.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.gslab.talent.model.Candidate;
import com.gslab.talent.model.Interview;
import com.gslab.talent.service.InterviewService;

@Controller
@CrossOrigin(allowedHeaders = "*")
@RequestMapping(value = "/v1")
public class InterviewController {

	@Autowired
	InterviewService interviewServiceObj;
	
	@GetMapping(value = "/getTodaysCandidateInterview")
	public ResponseEntity<List<Interview>> getTodaysCandidateInterview() {
		 List interview = interviewServiceObj.getTodaysInterview();
		 return new ResponseEntity<List<Interview>>(interview,HttpStatus.OK);
	}
	
	@GetMapping(value = "/getCandidateInterviewStatus")
	public ResponseEntity<String> getCandidateInterviewStatus() {
		 return new ResponseEntity<String>(interviewServiceObj.getInterviewStatus(),HttpStatus.OK);
	}
	
	@GetMapping(value = "/getNumberOfRejectedMonthly")
	public ResponseEntity<TreeMap<Integer, Integer>> getNumberOfRejectedMonthly() {
		 return new ResponseEntity<TreeMap<Integer, Integer>>(interviewServiceObj.getRejectedMonthly(),HttpStatus.OK);
	}
	
}