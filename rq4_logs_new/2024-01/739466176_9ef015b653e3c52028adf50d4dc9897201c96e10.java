package com.anghel.music_band_organizer.unit_test;

import com.anghel.music_band_organizer.models.dtos.UserDTO;
import com.anghel.music_band_organizer.models.entities.User;
import com.anghel.music_band_organizer.repository.UserRepository;
import com.anghel.music_band_organizer.services.user.UserServiceImpl;
import com.anghel.music_band_organizer.services.user.UserServiceValidationImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.modelmapper.ModelMapper;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class UserServiceImplTest {

    @Mock
    private final ModelMapper modelMapper = new ModelMapper();

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private UserServiceImpl userService;

    @Mock
    private UserServiceValidationImpl userServiceValidation;

    @Test
    void testCreateUser() {
        //GIVEN
        User user = new User();
        user.setFirstName("Crin");
        user.setLastName("Anton");
        user.setEmail("crin@gmail.com");
        user.setBirthday(LocalDate.parse("1998-03-11"));

        User savedUser = new User();
        savedUser.setId(1L);
        savedUser.setFirstName("Crin");
        savedUser.setLastName("Anton");
        savedUser.setEmail("crin@gmail.com");
        savedUser.setBirthday(LocalDate.parse("1998-03-11"));

        UserDTO userDTO = new UserDTO();
        userDTO.setId(1L);
        userDTO.setFirstName("Maria");
        userDTO.setLastName("Anton");
        userDTO.setEmail("crin@gmail.com");
        userDTO.setBirthday(LocalDate.parse("1998-03-11"));

        UserDTO returnedUserDTO = new UserDTO();
        returnedUserDTO.setId(1L);
        returnedUserDTO.setFirstName("Maria");
        returnedUserDTO.setLastName("Anton");
        returnedUserDTO.setEmail("crin@gmail.com");
        returnedUserDTO.setBirthday(LocalDate.parse("1998-03-11"));
        returnedUserDTO.setAge((int)ChronoUnit.YEARS.between(returnedUserDTO.getBirthday(), LocalDate.now()));

        when(modelMapper.map(returnedUserDTO, User.class)).thenReturn(user);
        when(modelMapper.map(savedUser, UserDTO.class)).thenReturn(returnedUserDTO);
        when(userRepository.save(user)).thenReturn(savedUser);

        //WHEN
        UserDTO savedUserDTO = userService.createUser(returnedUserDTO);

        //THEN
        verify(userRepository, times(1)).save(user);
        assertEquals(returnedUserDTO, savedUserDTO);
    }
}