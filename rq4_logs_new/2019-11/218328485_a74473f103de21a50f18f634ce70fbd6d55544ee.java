package com.project.teamup.mapper;

import com.project.teamup.dto.UserDTO;
import com.project.teamup.model.User;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    comments = "version: 1.3.0.Beta2, compiler: javac, environment: Java 1.8.0_172 (Oracle Corporation)"
)
@Component
public class UserMapperImpl extends UserMapper {

    @Override
    public User toUser(UserDTO userDto) {
        if ( userDto == null ) {
            return null;
        }

        User user = new User();

        user.setId( userDto.getId() );
        user.setUsername( userDto.getUsername() );
        user.setEmail( userDto.getEmail() );
        user.setFirstName( userDto.getFirstName() );
        user.setLastName( userDto.getLastName() );
        user.setPassword( userDto.getPassword() );
        user.setBirthDate( userDto.getBirthDate() );
        user.setRole( userDto.getRole() );

        return user;
    }

    @Override
    public List<User> toUserList(List<UserDTO> userDTOS) {
        if ( userDTOS == null ) {
            return null;
        }

        List<User> list = new ArrayList<User>( userDTOS.size() );
        for ( UserDTO userDTO : userDTOS ) {
            list.add( toUser( userDTO ) );
        }

        return list;
    }

    @Override
    public UserDTO toUserDto(User user) {
        if ( user == null ) {
            return null;
        }

        UserDTO userDTO = new UserDTO();

        userDTO.setId( user.getId() );
        userDTO.setUsername( user.getUsername() );
        userDTO.setPassword( user.getPassword() );
        userDTO.setEmail( user.getEmail() );
        userDTO.setFirstName( user.getFirstName() );
        userDTO.setLastName( user.getLastName() );
        userDTO.setBirthDate( user.getBirthDate() );
        userDTO.setRole( user.getRole() );

        return userDTO;
    }

    @Override
    public List<UserDTO> toUserDtoList(List<User> userDTOS) {
        if ( userDTOS == null ) {
            return null;
        }

        List<UserDTO> list = new ArrayList<UserDTO>( userDTOS.size() );
        for ( User user : userDTOS ) {
            list.add( toUserDto( user ) );
        }

        return list;
    }
}