package com.tgt.rysetii.learningresourcesapi.entity;
import java.time.LocalDate;

public class LearningResources {
	private Integer id;
    private String Name;
    private Double costPrice;
    private Double sellingPrice;
    private LearningResourcesStatus learningResourceStatus;
    private LocalDate createdDate;
    private LocalDate publishedDate;
    private LocalDate retiredDate;
    
    public LearningResources() 
    {
    }
    public Integer getLearningResourceId() {
        return id;
    }

    public void setLearningResourceId(Integer id) {
        this.id = id;
    }

    public String getProductName() {
        return Name;
    }

    public void setProductName(String productName) {
        this.Name = productName;
    }

    public Double getCostPrice() {
        return costPrice;
    }

    public void setCostPrice(Double costPrice) {
        this.costPrice = costPrice;
    }

    public Double getSellingPrice() {
        return sellingPrice;
    }

    public void setSellingPrice(Double sellingPrice) {
        this.sellingPrice = sellingPrice;
    }

    public LearningResourcesStatus getLearningResourceStatus() {
        return learningResourcesStatus;
    }

    public void setLearningResourceStatus(LearningResourcesStatus learningResourceStatus) {
        this.learningResourcesStatus = learningResourceStatus;
    }

    public LocalDate getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(LocalDate createdDate) {
        this.createdDate = createdDate;
    }

    public LocalDate getPublishedDate() {
        return publishedDate;
    }

    public void setPublishedDate(LocalDate publishedDate) {
        this.publishedDate = publishedDate;
    }

    public LocalDate getRetiredDate() {
        return retiredDate;
    }

    public void setRetiredDate(LocalDate retiredDate) {
        this.retiredDate = retiredDate;
    }
    
    public String toString() {
		return "LearningResource [id=" + id + ", name=" + Name + ", costPrice=" + costPrice + ", sellingPrice="
				+ sellingPrice + ", productStatus=" + learningResourceStatus + ", createdDate=" + createdDate
				+ ", publishedDate=" + publishedDate + ", retiredDate=" + retiredDate + "]";
	}
	
}