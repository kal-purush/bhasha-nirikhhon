package com.green.category.vo;

public class CategoryVo {
	// 주요 카테고리
	private int category_idx;
	private String category_name; // 노트북, 데스크탑, 컴퓨터부품, 주변부품

	public CategoryVo() {
	}

	public CategoryVo(int category_idx, String category_name) {
		super();
		this.category_idx = category_idx;
		this.category_name = category_name;
	}

	public int getCategory_idx() {
		return category_idx;
	}

	public void setCategory_idx(int category_idx) {
		this.category_idx = category_idx;
	}

	public String getCategory_name() {
		return category_name;
	}

	public void setCategory_name(String category_name) {
		this.category_name = category_name;
	}

	@Override
	public String toString() {
		return "CategoryVo [category_idx=" + category_idx + ", category_name=" + category_name + "]";
	};

}