package SAD.Database;

import org.apache.ibatis.annotations.Param;

import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface DAO4MyBatis {
    List<Map<String,Object>> selectMessage(@Param("receiverid") int receiverid);
    void insertMessage(@Param("fromuserid") int fromuserid, @Param("touserid") int touserid, @Param("content") String content, @Param("time") String time);
    List<Map<String,Object>>selectFollowedUser(@Param("userid") int userid);
    boolean ifFollow(@Param("userid") int userid, @Param("followid") int followid);
    void makeFollow(@Param("userid") int userid, @Param("followid") int followid);
    void unmakeFollow(@Param("userid") int userid, @Param("followid") int followid);
    boolean ifUserExistID(@Param("userid") int userid);
    boolean ifUserExistName(@Param("username") String username);
    boolean ifExpertExist(@Param("expertid") int expertid);
    boolean ifAdminExist(@Param("adminid") int adminid);
    boolean ifPhoneExist(@Param("phone") String phone);
    boolean ifIdentificationExist(@Param("identification") String identification);
    boolean selectUserAuthentication(@Param("userid") int userid, @Param("passwd") String passwd);
    void updateUserPasswd(@Param("userid") int userid, @Param("passwd") String passwd);
    void insertUser(@Param("name") String name, @Param("identification") String identification, @Param("cellphone") String cellphone, @Param("passwd") String passwd, @Param("email") String email);
    void updateEmail(@Param("userid") int userid, @Param("email") String email);
    void updatePhone(@Param("userid") int userid, @Param("phone") String phone);
    int selectPoint(@Param("userid") int userid);
    int selectUserId(@Param("username") String username);
    List<Map<String,Object>> selectUserInfo(@Param("userid") int userid);
    List<Map<String,Object>> selectUserResource(@Param("userid") int userid);
    List<Map<String,Object>> selectUserOrder(@Param("userid") int userid);
    void updateMoney(@Param("userid") int userid, @Param("addpoints") int addpoints);
    int selectMoney(@Param("userid") int userid);
}