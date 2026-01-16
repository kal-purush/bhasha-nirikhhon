package com.umc.accountbook.dto;

import lombok.Builder;
import lombok.Data;

@Data
public class CreateEssentialSpendingRequestDto {

    private int goal_id;
    private Long icon;
    private String content;
    private Long cost;

    public CreateEssentialSpendingRequestDto(int goal_id, Long icon, String content, Long cost) {
        this.goal_id = goal_id;
        this.icon = icon;
        this.content = content;
        this.cost = cost;
    }

    /*
    * {"goal_id": 1, "icon": 2, "content": "asas", "cost": 1}
    * */
}