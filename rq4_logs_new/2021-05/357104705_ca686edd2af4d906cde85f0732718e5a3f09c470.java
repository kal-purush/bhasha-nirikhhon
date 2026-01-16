package com.urlwiki.entities;


public class Article {

	
	public Article(int id, String websiteName, String category, String subCategory, String articleInfo, String url) {
		super();
		this.id = id;
		this.websiteName = websiteName;
		this.category = category;
		this.subCategory = subCategory;
		this.articleInfo = articleInfo;
		this.url = url;
	}
	
	private int id;
	private String websiteName;
	private String category;
	private String subCategory;
	private String articleInfo;
	private String url;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getWebsiteName() {
		return websiteName;
	}
	public void setWebsiteName(String websiteName) {
		this.websiteName = websiteName;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getSubCategory() {
		return subCategory;
	}
	public void setSubCategory(String subCategory) {
		this.subCategory = subCategory;
	}
	public String getArticleInfo() {
		return articleInfo;
	}
	public void setArticleInfo(String articleInfo) {
		this.articleInfo = articleInfo;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}

	
	
	
}