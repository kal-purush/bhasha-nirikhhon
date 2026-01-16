package by.bsuir.poit.bean.mappers;

import by.bsuir.poit.bean.Client;
import by.bsuir.poit.bean.User;
import org.mapstruct.*;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Paval Shlyk
 * @since 23/10/2023
 */
@Mapper(unmappedTargetPolicy = ReportingPolicy.ERROR, componentModel = MappingConstants.ComponentModel.JAKARTA)
public interface ClientUserMapper extends ResultSetMapper<User> {
@Mapping(target = "ranking", ignore = true)
@Mapping(target = "account", ignore = true)
Client updateClientWithParent(@MappingTarget Client client, User user);

@Override
default User fromResultSet(ResultSet set) throws SQLException {
      return User.builder()
		 .id(set.getLong("user_id"))
		 .name(set.getString("name"))
		 .phoneNumber(set.getString("phone_number"))
		 .email(set.getString("email"))
		 .role(set.getShort("role"))
		 .status(set.getShort("status"))
		 .passwordHash(set.getString("password_hash"))
		 .securitySalt(set.getString("security_salt"))
		 .build();
}

default Client fromResultSetClient(ResultSet set) throws SQLException {
      User user = fromResultSet(set);
      Client client = Client.builder()
			  .account(set.getDouble("account"))
			  .ranking(set.getDouble("ranking"))
			  .build();
      return updateClientWithParent(client, user);
}

@Mapping(target = "ranking", constant = "0.0")
@Mapping(target = "account", constant = "0.0")
Client fromUser(User user);
}