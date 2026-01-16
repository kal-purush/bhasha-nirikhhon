package groupone.userservice.service;

import groupone.userservice.dao.UserDao;
import groupone.userservice.dto.request.RegisterRequest;
import groupone.userservice.entity.User;
import groupone.userservice.entity.UserType;
import org.apache.http.auth.InvalidCredentialsException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;


@ExtendWith(MockitoExtension.class)
public class UserServiceTest {

    @Mock
    private UserDao userDao;

    @InjectMocks
    private UserService userService;
    String sDate1="7/12/2014";
    Date date1=new SimpleDateFormat("MM/dd/yyyy").parse(sDate1);
    String sDate2="4/21/2014";
    Date date2=new SimpleDateFormat("MM/dd/yyyy").parse(sDate2);
    private User user1 = new User(1,"john@example.com","John","Doe","123", date1, true, 1,"https://drive.google.com/file/d/1Ul78obBTS0zgaVOufCHpUKwMxBvDON-i/view", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIiwiZXhwIjoxNjg5ODkyMjc0fQ.bMc7kDmKL92H3DJleE7G7u9v0Y98KLDk4qPjPEbZdoo");
    private User user2 = new User(2,"jane@example.com","Jane","Smith","123", date2, true,2,"url-new","eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIzIiwiZXhwIjoxNjkwMDU3MzcwfQ.mWBG0g25cfv9K-rn-XuScThmUx4KEU04323-6kaZDZI");

    public UserServiceTest() throws ParseException {
    }

    @Test
    public void test_getAllUsers(){
        List<User> expected = new ArrayList<>();
        expected.add(user1);
        expected.add(user2);

        Mockito.when(userDao.getAllUsers()).thenReturn(expected);

        assertEquals(expected, userService.getAllUsers());
    }

    @Test
    public void test_getUserById() {
        Integer userId = 1;
        User user = user1;
        Mockito.when(userDao.getUserById(userId)).thenReturn(user);

        User actualUser = userService.getUserById(userId);

        assertEquals(user, actualUser);
    }

    @Test
    public void testLoadUserByUsername_UserNotFound() {
        String email = "notfound@example.com";
        Mockito.when(userDao.loadUserByEmail(email)).thenReturn(Optional.empty());

        assertThrows(UsernameNotFoundException.class, () -> userService.loadUserByUsername(email));
    }

    @Test
    public void testLoadUserByUsername() {
        String email = "johndoe@example.com";
        User user = user1;
        user.setEmail(email);
        Mockito.when(userDao.loadUserByEmail(email)).thenReturn(Optional.of(user));

        UserDetails userDetails = userService.loadUserByUsername(email);

        assertNotNull(userDetails);
        assertEquals(email, userDetails.getUsername());
        assertEquals(true, new BCryptPasswordEncoder().matches(user.getPassword(), userDetails.getPassword()));

        assertTrue(userDetails.isEnabled());
    }
    @Test
    public void testGetAuthoritiesFromUser_NormalUser() {
        User user = new User();
        user.setType(UserType.NORMAL_USER.ordinal());

        List<GrantedAuthority> authorities = userService.getAuthoritiesFromUser(user);

        assertNotNull(authorities);
        assertTrue(authorities.contains(new SimpleGrantedAuthority("read")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("write")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("delete")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("update")));
    }
    @Test
    public void testGetAuthoritiesFromUser_NormalUserNotValid() {
        User user = new User();
        user.setType(UserType.NORMAL_USER_NOT_VALID.ordinal());

        List<GrantedAuthority> authorities = userService.getAuthoritiesFromUser(user);

        assertNotNull(authorities);
        assertTrue(authorities.contains(new SimpleGrantedAuthority("read")));
        assertFalse(authorities.contains(new SimpleGrantedAuthority("write")));
        assertFalse(authorities.contains(new SimpleGrantedAuthority("delete")));
        assertFalse(authorities.contains(new SimpleGrantedAuthority("update")));
    }

    @Test
    public void testGetAuthoritiesFromUser_Admin() {
        User user = new User();
        user.setType(UserType.ADMIN.ordinal());

        List<GrantedAuthority> authorities = userService.getAuthoritiesFromUser(user);

        assertNotNull(authorities);
        assertTrue(authorities.contains(new SimpleGrantedAuthority("read")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("admin_read")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("delete")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("ban_unban")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("recover")));
        assertFalse(authorities.contains(new SimpleGrantedAuthority("update")));
    }

    @Test
    public void testGetAuthoritiesFromUser_SuperAdmin() {
        User user = new User();
        user.setType(UserType.SUPER_ADMIN.ordinal());

        List<GrantedAuthority> authorities = userService.getAuthoritiesFromUser(user);

        assertNotNull(authorities);
        assertTrue(authorities.contains(new SimpleGrantedAuthority("read")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("admin_read")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("delete")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("ban_unban")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("recover")));
        assertTrue(authorities.contains(new SimpleGrantedAuthority("promote")));
        assertFalse(authorities.contains(new SimpleGrantedAuthority("update")));
    }

    @Test
    public void test_existsByEmail_EmailExists() {
        String email = "john@example.com";
        Mockito.when(userDao.loadUserByEmail(email)).thenReturn(Optional.of(new User()));

        boolean result = userService.existsByEmail(email);

        assertTrue(result);
    }
    @Test
    public void test_existsByEmail_EmailDoesNotExist() {
        String email = "nonexistent@example.com";
        Mockito.when(userDao.loadUserByEmail(email)).thenReturn(Optional.empty());

        boolean result = userService.existsByEmail(email);

        assertFalse(result);
    }
    @Test
    public void test_addUser_Success() throws InvalidCredentialsException {
        RegisterRequest request = new RegisterRequest("John", "Doe", "john@example.com", "123", "url");

        // Set other fields if needed...

        Mockito.when(userDao.loadUserByEmail(request.getEmail())).thenReturn(Optional.empty());
        Mockito.when(userDao.getUserById(anyInt())).thenReturn(user1);
        Mockito.when(userDao.addUser(any(User.class))).thenReturn(1); // Return the user ID for testing purposes

        String token = userService.addUser(request);
        assertNotNull(token);
        // Additional assertions can be added here to check the user object's properties and token validity
    }
}