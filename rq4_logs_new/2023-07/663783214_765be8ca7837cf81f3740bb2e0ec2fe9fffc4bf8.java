package com.example.jalihara;

public class Item {


//    private int itemIdx;
    private int itemImage;
    private String itemDate;
    private String itemTitle;
    private String itemDescription;
    private String itemPrice;

    public Item(int itemImage, String itemDate, String itemTitle, String itemDescription, String itemPrice) {
//        this.itemIdx = itemIdx;
        this.itemImage = itemImage;
        this.itemDate = itemDate;
        this.itemTitle = itemTitle;
        this.itemDescription = itemDescription;
        this.itemPrice = itemPrice;
    }

//    public int getItemIdx() {
//        return itemIdx;
//    }
//
//    public void setItemIdx(int itemIdx) {
//        this.itemIdx = itemIdx;
//    }

    public int getItemImage() {
        return itemImage;
    }

    public void setItemImage(int itemImage) {
        this.itemImage = itemImage;
    }

    public String getItemDate() {
        return itemDate;
    }

    public void setItemDate(String itemDate) {
        this.itemDate = itemDate;
    }

    public String getItemTitle() {
        return itemTitle;
    }

    public void setItemTitle(String itemTitle) {
        this.itemTitle = itemTitle;
    }

    public String getItemDescription() {
        return itemDescription;
    }

    public void setItemDescription(String itemDescription) {
        this.itemDescription = itemDescription;
    }

    public String getItemPrice() {
        return itemPrice;
    }

    public void setItemPrice(String itemPrice) {
        this.itemPrice = itemPrice;
    }
}