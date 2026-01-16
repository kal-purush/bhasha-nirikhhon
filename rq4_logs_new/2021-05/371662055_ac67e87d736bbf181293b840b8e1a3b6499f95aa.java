package com.challenge.transactions.controller;

import static org.junit.Assert.assertFalse;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import com.challenge.transactions.models.Statistics;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
class StatisticsControllerTest {

	@Autowired
	Validator validator;

	@Autowired
	private MockMvc mockMvc;

	@Test
	public void getRequest_InternalServerError_IfSumIsNull() {
		Statistics statistics = new Statistics(null, new BigDecimal("100.53"), new BigDecimal("200000.49"),
				new BigDecimal("50.23"), Long.valueOf(10));

		Set<ConstraintViolation<Statistics>> constraintViolations = validator.validate(statistics);
		assertFalse(constraintViolations.isEmpty());
	}

	@Test
	public void getRequest_InternalServerError_IfAvgIsNull() {
		Statistics statistics = new Statistics(new BigDecimal("1000.00"), null, new BigDecimal("200000.49"),
				new BigDecimal("50.23"), Long.valueOf(10));

		Set<ConstraintViolation<Statistics>> constraintViolations = validator.validate(statistics);
		assertFalse(constraintViolations.isEmpty());
	}

	@Test
	public void getRequest_InternalServerError_IfMaxIsNull() {
		Statistics statistics = new Statistics(new BigDecimal("1000.00"), new BigDecimal("100.53"), null,
				new BigDecimal("50.23"), Long.valueOf(10));

		Set<ConstraintViolation<Statistics>> constraintViolations = validator.validate(statistics);
		assertFalse(constraintViolations.isEmpty());
	}

	@Test
	public void getRequest_InternalServerError_IfMinIsNull() {
		Statistics statistics = new Statistics(null, new BigDecimal("100.53"), new BigDecimal("200000.49"), null,
				Long.valueOf(10));

		Set<ConstraintViolation<Statistics>> constraintViolations = validator.validate(statistics);
		assertFalse(constraintViolations.isEmpty());
	}

	@Test
	public void getRequest_InternalServerError_IfCountIsNull() {
		Statistics statistics = new Statistics(null, new BigDecimal("100.53"), new BigDecimal("200000.49"),
				new BigDecimal("50.23"), null);

		Set<ConstraintViolation<Statistics>> constraintViolations = validator.validate(statistics);
		assertFalse(constraintViolations.isEmpty());
	}

	@Test
	void TestGetStatistics() throws Exception {
		mockMvc.perform(get("/statistics")).andExpect(status().isOk());

	}

}