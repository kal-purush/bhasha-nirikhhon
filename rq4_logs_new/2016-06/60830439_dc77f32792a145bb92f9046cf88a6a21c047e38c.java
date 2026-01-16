package easyrun.shopping.model;

import java.io.Serializable;

/**
 * Created by sunfusheng on 16/4/20.
 */
public class ClothesEntity implements Serializable, Comparable<ClothesEntity> {

    private String brand = "";		//品牌
    private String name = "";		//衣服名
    private String describe = "";	//描述
    private String imgURL = "";		//图片来源URL
    private String saleNumber = "0";//销量
    private String price = "";		//价格（积分）
    private String type = "";		//类别


    // 暂无数据属性
    private boolean isNoData = false;
    private int height;

    public ClothesEntity() {
        this.brand = "";
        this.name = "";
        this.describe = "";
        this.imgURL = "";
        this.saleNumber = "0";
        this.price = "0";
        this.type = "";
    }

    public ClothesEntity(String brand, String name, String describe,
                         String imgURL, String saleNumber, String price, String type) {
        this.brand = brand;
        this.name = name;
        this.describe = describe;
        this.imgURL = imgURL;
        this.saleNumber = saleNumber;
        this.price = price;
        this.type = type;

    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public boolean isNoData() {
        return isNoData;
    }

    public void setNoData(boolean noData) {
        isNoData = noData;
    }



    @Override
    public int compareTo(ClothesEntity another) {
        return Integer.parseInt(this.getSaleNumber()) - Integer.parseInt(another.getSaleNumber());
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescribe() {
        return describe;
    }

    public void setDescribe(String describe) {
        this.describe = describe;
    }

    public String getImgURL() {
        return imgURL;
    }

    public void setImgURL(String imgURL) {
        this.imgURL = imgURL;
    }

    public String getSaleNumber() {
        return saleNumber;
    }

    public void setSaleNumber(String saleNumber) {
        this.saleNumber = saleNumber;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}