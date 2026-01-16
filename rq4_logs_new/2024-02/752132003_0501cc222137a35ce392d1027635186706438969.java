package com.web.domain;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Builder
@Entity
@Table(name = "KYOBOCRAWL")
public class KyoboBookCrawl {
	
	@Id
	private String bookName;
	
	private int bookPrice;
	private String viewDetail;
	private String imageName;
	
	public KyoboBookCrawl() {
		
	}
	
	public KyoboBookCrawl(String bookName, int bookPrice, String viewDetail, String imageName) {
		super();
		this.bookName = bookName;
		this.bookPrice = bookPrice;
		this.viewDetail = viewDetail;
		this.imageName = imageName;
	}
	
	

}