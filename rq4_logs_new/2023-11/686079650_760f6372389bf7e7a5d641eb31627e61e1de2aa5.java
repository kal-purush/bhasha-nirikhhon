package by.bsuir.poit.bean.mappers;

import by.bsuir.poit.bean.Client;
import by.bsuir.poit.bean.User;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Paval Shlyk
 * @since 08/11/2023
 */
@Mapper(unmappedTargetPolicy = ReportingPolicy.ERROR, componentModel = MappingConstants.ComponentModel.JAKARTA)
public interface ClientMapper extends ResultSetMapper<Client> {
@Mapping(target = "ranking", constant = "0.0")
@Mapping(target = "account", constant = "0.0")
Client fromUser(User user);

@Override
default Client fromResultSet(ResultSet set) throws SQLException {
      return Client.builder()
		 .id(set.getLong("user_id"))
		 .name(set.getString("name"))
		 .account(set.getDouble("account"))
		 .ranking(set.getDouble("ranking"))
		 .build();
}
}