package com.capg;

import org.junit.Assert;
import org.junit.Test;

import com.capg.CensusAnalyserException.ExceptionType;

public class CensusAnalyserTest {
	StateCensusAnalyser censusAnalyser = new StateCensusAnalyser();
	private static final String INDIA_CENSUS_CSVFILE_PATH="C:\\Users\\lkvre\\eclipse-workspace\\IndianCensusAnalyser\\src\\test\\resources\\IndiaStateCensusData.csv";
	private static final String WRONG_CSV_FILE_PATH="./src/main/resources/IndiaStateCensusData.csv";
	
	@Test
	public void givenCensusData_WithWrongFileFormat_ShouldThrowException() {
		try {
			censusAnalyser.loadCensusData("CSVFile.txt");
		}
		catch(CensusAnalyserException e) {
			System.out.println(e);
			Assert.assertEquals(ExceptionType.INVALID_FILE_FORMAT, e.exceptionType);
		}
	}
}