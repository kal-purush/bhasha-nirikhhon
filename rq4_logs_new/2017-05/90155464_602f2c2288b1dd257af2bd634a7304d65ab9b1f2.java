package com.sconfield.daisy.model;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

public class MongoPage implements Pageable {
	
	private Integer currentPage = 1;
	private Integer onePageSize = 10;
	private Sort currentSort;

	// getter and setter.
	public Integer getCurrentPage() {
		return currentPage;
	}

	public void setCurrentPage(Integer currentPage) {
		this.currentPage = currentPage;
	}

	public Integer getOnePageSize() {
		return onePageSize;
	}

	public void setOnePageSize(Integer onePageSize) {
		this.onePageSize = onePageSize;
	}

	public Sort getCurrentSort() {
		return currentSort;
	}

	public void setCurrentSort(Sort currentSort) {
		this.currentSort = currentSort;
	}

	@Override
	public Pageable first() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getOffset() {
		// TODO Auto-generated method stub
		return (this.getCurrentPage()-1)*this.getOnePageSize();
	}

	@Override
	public int getPageNumber() {
		return this.getCurrentPage();
	}

	@Override
	public int getPageSize() {
		return this.getOnePageSize();
	}

	@Override
	public Sort getSort() {
		return this.getCurrentSort();
	}

	@Override
	public boolean hasPrevious() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Pageable next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Pageable previousOrFirst() {
		// TODO Auto-generated method stub
		return null;
	}

}