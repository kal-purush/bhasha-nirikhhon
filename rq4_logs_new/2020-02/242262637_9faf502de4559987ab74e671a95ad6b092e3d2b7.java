package com.seblong.okr.controllers;

import java.util.Collections;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.seblong.okr.entities.OKR.Objective;
import com.seblong.okr.entities.OKRPeriod;
import com.seblong.okr.exceptions.ValidationException;
import com.seblong.okr.resource.StandardEntityResource;
import com.seblong.okr.resource.StandardListResource;
import com.seblong.okr.resource.StandardRestResource;
import com.seblong.okr.services.OKRPeriodService;
import com.seblong.okr.services.OKRService;

@Controller
@RequestMapping(value = "/okr", produces = MediaType.APPLICATION_JSON_VALUE)
public class APIOKRController {

	@Autowired
	private OKRService okrService;

	@Autowired
	private OKRPeriodService okrPeriodService;

	@PostMapping(value = "/objective/add")
	public ResponseEntity<StandardRestResource> addObjective(@RequestParam(value = "user", required = true) String user,
			@RequestParam(value = "period", required = true) String period,
			@RequestParam(value = "title", required = true) String title,
			@RequestParam(value = "estimate", required = true) long estimate,
			@RequestParam(value = "confidence", required = true) double confidence) {

		validateObjective(user, period, title, estimate, confidence);
		Objective objective = okrService.addObjective(user, period, title, estimate, confidence);
		return new ResponseEntity<StandardRestResource>(new StandardEntityResource<Objective>(objective),
				HttpStatus.OK);
	}

	@GetMapping(value = "/objective/list")
	public ResponseEntity<StandardRestResource> listObjective(
			@RequestParam(value = "user", required = true) String user,
			@RequestParam(value = "period", required = true) String period) {
		List<Objective> objectives = okrService.listObjectives(user, period);
		if (objectives == null) {
			objectives = Collections.emptyList();
		}
		return new ResponseEntity<StandardRestResource>(new StandardListResource<Objective>(objectives), HttpStatus.OK);
	}

	@PostMapping(value = "/objective/update")
	public ResponseEntity<StandardRestResource> updateObjective(
			@RequestParam(value = "user", required = true) String user,
			@RequestParam(value = "period", required = true) String period,
			@RequestParam(value = "id", required = true) String id,
			@RequestParam(value = "title", required = true) String title,
			@RequestParam(value = "estimate", required = true) long estimate,
			@RequestParam(value = "confidence", required = true) double confidence) {

		validateObjective(user, period, title, estimate, confidence);
		Objective objective = okrService.updateObjective(user, period, id, title, estimate, confidence);
		if (objective == null) {
			throw new ValidationException(404, "objective-not-exist");
		}
		return new ResponseEntity<StandardRestResource>(new StandardEntityResource<Objective>(objective),
				HttpStatus.OK);
	}

	@PostMapping(value = "/objective/delete")
	public ResponseEntity<StandardRestResource> deleteObjective(
			@RequestParam(value = "user", required = true) String user,
			@RequestParam(value = "period", required = true) String period,
			@RequestParam(value = "id", required = true) String id) {
		validateUser(user);
		okrService.deleteObjective(user, period, id);
		return new ResponseEntity<StandardRestResource>(new StandardRestResource(200, "OK"), HttpStatus.OK);
	}

	@PostMapping(value = "/keyresult/add")
	public ResponseEntity<StandardRestResource> addKeyResult(@RequestParam(value = "user", required = true) String user,
			@RequestParam(value = "period", required = true) String period,
			@RequestParam(value = "objective", required = true) String objective,
			@RequestParam(value = "title", required = true) String title,
			@RequestParam(value = "estimate", required = true) long estimate,
			@RequestParam(value = "confidence", required = true) double confidence,
			@RequestParam(value = "weight", required = true) double weight) {
		validateKeyResult(user, period, objective, title, estimate, confidence, weight);
		Objective object = okrService.addKeyResult(user, period, objective, title, estimate, confidence, weight);
		return new ResponseEntity<StandardRestResource>(new StandardEntityResource<Objective>(object), HttpStatus.OK);
	}

	@PostMapping(value = "/keyresult/update")
	public ResponseEntity<StandardRestResource> updateKeyResult(@RequestParam(value = "user", required = true) String user,
			@RequestParam(value = "period", required = true) String period,
			@RequestParam(value = "objective", required = true) String objective,
			@RequestParam(value = "id", required = true) String id,
			@RequestParam(value = "title", required = true) String title,
			@RequestParam(value = "score", required = true) double score,
			@RequestParam(value = "estimate", required = true) long estimate,
			@RequestParam(value = "confidence", required = true) double confidence,
			@RequestParam(value = "weight", required = true) double weight) {
		validateKeyResult(user, period, objective, title, estimate, confidence, weight);
		Objective object = okrService.updateKeyResult(user, period, objective, id, title, score, estimate, confidence,
				weight);
		return new ResponseEntity<StandardRestResource>(new StandardEntityResource<Objective>(object), HttpStatus.OK);
	}

	@PostMapping(value = "/keyresult/delete")
	public ResponseEntity<StandardRestResource> deleteKeyResult(@RequestParam(value = "user", required = true) String user,
			@RequestParam(value = "period", required = true) String period,
			@RequestParam(value = "objective", required = true) String objective,
			@RequestParam(value = "id", required = true) String id) {
		validateUser(user);
		validatePeriod(period, 0);
		okrService.deleteKeyResult(user, period, objective, id);
		return new ResponseEntity<StandardRestResource>(new StandardRestResource(200, "OK"), HttpStatus.OK);
	}

	@PostMapping(value = "/objective/update")
	public ResponseEntity<StandardRestResource> updateProgress(@RequestParam(value = "user", required = true) String user,
			@RequestParam(value = "period", required = true) String period,
			@RequestParam(value = "objective", required = true) String objective,
			@RequestParam(value = "progress", required = true)String progress) {
		validateUser(user);
		validatePeriod(period, 0);
		okrService.updateProgress(user, period, objective, progress);
		return new ResponseEntity<StandardRestResource>(new StandardRestResource(200, "OK"), HttpStatus.OK);
	}

	@PostMapping(value = "/keyresult/score")
	public ResponseEntity<StandardRestResource> scoreKeyResult(@RequestParam(value = "user", required = true) String user,
			@RequestParam(value = "period", required = true) String period,
			@RequestParam(value = "objective", required = true) String objective,
			@RequestParam(value = "id", required = true) String id,
			@RequestParam(value = "score", required = true) double score) {
		validateUser(user);
		validatePeriod(period, 0);
		validateScore(score);
		Objective object = okrService.scoreKeyResult(user, period, objective, id, score);
		return new ResponseEntity<StandardRestResource>(new StandardEntityResource<Objective>(object), HttpStatus.OK);
	}

	private void validateObjective(String user, String period, String title, long estimate, double confidence)
			throws ValidationException {
		validateUser(user);
		validatePeriod(period, estimate);
		validateTitle(title);
		validateConfidence(confidence);
	}

	private void validateKeyResult(String user, String period, String objective, String title, long estimate,
			double confidence, double weight) {
		validateUser(user);
		validatePeriod(period, estimate);
		validateTitle(title);
		validateConfidence(confidence);
		validateWeight(weight);
	}

	private void validateUser(String user) {
		if (!ObjectId.isValid(user)) {
			throw new ValidationException(404, "user-not-exist");
		}
	}

	private void validatePeriod(String period, long estimate) {
		if (!ObjectId.isValid(period)) {
			throw new ValidationException(404, "period-not-exist");
		}
		OKRPeriod okrPeriod = okrPeriodService.get(period);
		if (period == null) {
			throw new ValidationException(404, "period-not-exist");
		}
		if (estimate > 0 && (estimate <= okrPeriod.getStart() || estimate >= okrPeriod.getEnd())) {
			throw new ValidationException(400, "invalid-period");
		}
	}

	private void validateTitle(String title) {
		if (StringUtils.isEmpty(title)) {
			throw new ValidationException(400, "invalid-title");
		}
	}

	private void validateConfidence(double confidence) {
		if (confidence < 0 || confidence > 1) {
			throw new ValidationException(400, "invalid-confidence");
		}
	}

	private void validateScore(double score) {
		if (score < 0 || score > 10) {
			throw new ValidationException(400, "invalid-score");
		}
	}

	private void validateWeight(double weight) {
		if (weight < 0 || weight > 1) {
			throw new ValidationException(400, "invalid-weight");
		}
	}

}