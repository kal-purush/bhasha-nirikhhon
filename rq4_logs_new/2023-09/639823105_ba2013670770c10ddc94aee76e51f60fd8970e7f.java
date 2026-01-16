package com.teachsync.dtos.room;

import com.teachsync.dtos.BaseCreateDTO;
import com.teachsync.dtos.BaseReadDTO;
import com.teachsync.dtos.center.CenterReadDTO;
import com.teachsync.utils.enums.RoomType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * DTO for {@link com.teachsync.entities.Room}
 */
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RoomCreateDTO extends BaseCreateDTO {

    private Long centerId;

    private CenterReadDTO center;

    private RoomType roomType;

    private String roomDesc;

    private String roomName;

    private Integer roomSize;

}