package by.bsuir.poit.bean.mappers;

import by.bsuir.poit.bean.AuctionMember;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Paval Shlyk
 * @since 12/11/2023
 */
@Mapper(unmappedSourcePolicy = ReportingPolicy.ERROR, componentModel = MappingConstants.ComponentModel.JAKARTA)
public interface AuctionMemberMapper extends ResultSetMapper<AuctionMember> {
@Override
default AuctionMember fromResultSet(ResultSet set) throws SQLException {
      return AuctionMember.builder()
		 .auctionId(set.getLong("auction_id"))
		 .clientId(set.getLong("client_id"))
		 .status(set.getShort("status"))
		 .build();
}
}