package com.atable.alcholknowledge.dto;

import com.atable.alcholknowledge.model.WineInfo;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

@Getter
public class WineInfoDto {
    private Long pk;
    private String nameKor;
    private String nameEng;
    private Integer vintage;
    private Integer price;
    private String sizeBottle;
    private String region;
    private String store;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private Date datePurchase;
    private String description;

    public WineInfoDto (WineInfo wineInfo) {
        this.pk = wineInfo.getPk();
        this.nameEng = wineInfo.getNameEng();
        this.nameKor = wineInfo.getNameKor();
        this.price = wineInfo.getPrice();
        this.vintage = wineInfo.getVintage();
        this.store = wineInfo.getStore();
        this.region = wineInfo.getRegion();
        this.datePurchase = wineInfo.getDatePurchase();
        this.description = wineInfo.getDescription();
        this.sizeBottle = wineInfo.getSizeBottle();
    }
}