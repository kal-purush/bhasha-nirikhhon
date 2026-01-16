package com.onlineschool.demo.course;

import java.math.BigDecimal;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Digits;
import javax.validation.constraints.NotNull;

public class SchoolCourse {
	
	@NotNull(message="is required")
	private String subjectName;
	
	@NotNull(message="is required")
	@DecimalMin(value="1.0", inclusive=true)
	@Digits(integer=3, fraction=2)
	private BigDecimal price;

	public SchoolCourse() {
		super();
	}

	public String getSubjectName() {
		return subjectName;
	}

	public void setSubjectName(String subjectName) {
		this.subjectName = subjectName;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}
}