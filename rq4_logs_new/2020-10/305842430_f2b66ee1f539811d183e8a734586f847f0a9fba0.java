package com.capg;

import java.io.Reader;
import java.util.Iterator;
import java.util.List;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

public class OpenCSVBuilder<E> implements ICSVBuilder {
	public Iterator<E> getCSVFileIterator(Reader reader, Class csvClass) throws CSVException {
		try {
			return this.beanGenerator(reader, csvClass).iterator();
		}
		catch(IllegalStateException e) {
			throw new CSVException("Unable to Parse", CSVException.ExceptionType.UNABLE_TO_PARSE);
		}
	}
	
	public List<E> getCSVList(Reader reader, Class csvClass) throws CSVException{
		return this.beanGenerator(reader, csvClass).parse();
	}
	
	private CsvToBean<E> beanGenerator(Reader reader, Class csvClass) throws CSVException{
		try {
			CsvToBeanBuilder<E> csvToBeanBuilder = new CsvToBeanBuilder<>(reader);
			csvToBeanBuilder.withType(csvClass);
			csvToBeanBuilder.withIgnoreLeadingWhiteSpace(true);
			CsvToBean<E> csvToBean = csvToBeanBuilder.build();
			return csvToBean;
		}
		catch(IllegalStateException e) {
			throw new CSVException("Unable to Parse", CSVException.ExceptionType.UNABLE_TO_PARSE);
		}
	}
}