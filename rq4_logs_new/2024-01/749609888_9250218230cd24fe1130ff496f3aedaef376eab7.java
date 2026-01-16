package com.example.app.domain;

import java.time.LocalDate;

import lombok.Data;

@Data
public class Record {

	private int trainingId;
	private int userId;
	private float userWeight;
	private float bmi;
	private LocalDate date;
}