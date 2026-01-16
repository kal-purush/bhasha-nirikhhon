package com.bearAndPupperCo.sangenWrestlingApp.Mappers;

import com.bearAndPupperCo.sangenWrestlingApp.DTO.WrestlerMainTableDTO;
import com.bearAndPupperCo.sangenWrestlingApp.Entities.LockerRoom;
import com.bearAndPupperCo.sangenWrestlingApp.Entities.WrestlingBrand;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class WrestlerMainTableDTOMapper implements RowMapper<WrestlerMainTableDTO> {

    @Override
    public WrestlerMainTableDTO map(ResultSet rs, StatementContext ctx) throws SQLException {
        WrestlerMainTableDTO wrestler = new WrestlerMainTableDTO();
        WrestlingBrand brand = new WrestlingBrand();
        LockerRoom lockerRoom = new LockerRoom();

        wrestler.setInRingName(rs.getString("in_ring_name"));
        wrestler.setNumberOfWins(rs.getInt("total_victories"));
        wrestler.setNumberOfLosses(rs.getInt("total_losses"));
        wrestler.setWrestlingStreak(rs.getInt("wrestling_streak"));
        wrestler.setPercentageOfWins(rs.getDouble("percentage_of_victories"));

        brand.setWrestlingBrandId(rs.getLong("wrestling_brand_id"));
        brand.setWrestlingBrandName(rs.getString("wrestling_brand_name"));

        lockerRoom.setWrestlingLockerRoomId(rs.getLong("wrestling_locker_room_id"));
        lockerRoom.setWrestlingLockerRoomName(rs.getString("wrestling_locker_room_name"));

        wrestler.setBrand(brand);
        wrestler.setLockerRoom(lockerRoom);

        return wrestler;
    }
}