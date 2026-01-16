package com.autohome.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @program: car_home
 * @description:
 * @author: ztk
 * @create: 2020-08-10 01:33
 **/

@NoArgsConstructor
@AllArgsConstructor
@Data
public class UsedCarGeneralDto {
    private String style_description;
    private Double car_price;
    private String car_registration_time;
    private Double performance_mileage;
    private Integer change_number;
    private String license_location;
    private String meeting_place;
    private Integer uid;
    private Integer cid;
    private Integer uniqueId;
}