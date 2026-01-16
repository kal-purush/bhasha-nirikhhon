package com.user.dao.mapper.test;

import com.register.model.Sex;
import com.register.model.User;
import com.user.dao.mapper.UserMapper;
import javax.transaction.Transactional;
import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Mockito.mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Test mapper for UserMapper
 * 
 * @Shaoxing Pang
 */
@RunWith(MockitoJUnitRunner.class)
public class UserMapperTest extends TestCase {
    
    private static UserMapper userMapper;
    
    public UserMapperTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {

    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        userMapper = mock(UserMapper.class);
    }
    
    @Test
    public void testAddUserMapper(){
        User user = new User();
        user.setUserName("user-test");
        user.setPassWord("123456");
        user.setSex(Sex.MALE);
        int result = userMapper.addUser(user);
        assertEquals(0,result);
    }
    
    @Test
    public void testJudgeUserMapper(){
        String username="123456";
        User user = userMapper.userJudge(username);
        assertNotNull(user);
    }
    
    @Test
    public void testLoginUserMapper(){
        String username = "123456";
        String password = "123456";
        User user = userMapper.userLogin(username,password);
        assertNotNull(user);
    }
    
}