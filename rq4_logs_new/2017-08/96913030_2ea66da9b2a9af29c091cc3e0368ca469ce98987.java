package cn.pers.chinaSoft.until;

import org.apache.ibatis.annotations.Insert;
import cn.pers.chinaSoft.bean.User;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.*;
/**
 * 用户注解接口
 * Created by 登祥 on 2017/8/2.
 */
public interface Auser {

    /**
     * 查询所有用户信息
     * @return List<User>
     */
    @Select("select * from DENG_USERINFO")
    public List<User> query();

    /**
     * 获得用户总数
     * @return List<User>
     */
    @Select("select count(1) NUM from DENG_USERINFO")
    public List<Map<String,Object>> getTotal();


    /**
     * 用户注册
     * @return int
     */
    @Insert("INSERT into DENG_USERINFO(USER_ID,USER_ACCOUNT,USER_PASSWORD,PHONE,EMAIL,USER_NAME,DEPT_ID,STATE,USER_NOTE) VALUES(#{user_id},#{user_account},#{user_password},#{phone},#{email},#{user_name},#{dept_id},#{state},#{user_note})")
    public int addUser(User user);

    /**
     * 登陆验证
     * @return List<User>
     */
    @Select("select * from DENG_USERINFO where USER_ACCOUNT = #{user_account} and USER_PASSWORD = #{user_password}")
    public List<User> userLogin(User user);



    /**
     * 获取当前用户的权限信息
     * @return
     */
    @Select("SELECT u4.USER_NAME, o1.OPERATION_ID, o1.OPERATION_NAME,o1.PID, o1.OPERATION_PATH\n" +
            "FROM DEND_OPERATION o1 INNER JOIN (SELECT u3.USER_NAME,r1.OPERSTION_ID\n" +
            "                                   FROM DENG_ROLE_OPERATION r1 INNER JOIN (SELECT u1.USER_NAME, u2.USER_GROUP_ID FROM DENG_USERINFO_ROLE u2 INNER JOIN (SELECT *\n" +
            "FROM DENG_USERINFO WHERE USER_ACCOUNT = #{user_account} and USER_PASSWORD = #{user_password}) u1\n" +
            " ON u1.USER_ID = u2.USER_ID) u3\n" +
            " ON u3.USER_GROUP_ID = r1.USERGROUP_ID) u4 ON u4.OPERSTION_ID = o1.OPERATION_ID ORDER BY o1.NO")
    public List<Map<String,Object>> showUserOper(User user);

    /**
     * 修改密码
     * @param user
     * @return int
     */
    @Update("update DENG_USERINFO u SET u.USER_PASSWORD=#{user_password} where u.USER_ACCOUNT=#{user_account}")
    public int updatepass(User user);

    /**
     * 查看指定用户的身份
     * @param user
     * @return
     */
    @Select("SELECT r1.ROLE_NAME FROM DENG_ROLEINFO r1 INNER JOIN (SELECT r.USER_GROUP_ID ugi from DENG_USERINFO_ROLE r INNER JOIN (select u.USER_ID from DENG_USERINFO u where u.USER_ACCOUNT = #{user_account}) u1 on u1.USER_ID = r.USER_ID) u2 ON u2.ugi =r1.ROLE_ID")
    public List<Map<String,Object>> queryUserRole(User user);

    /**
     * 查询所有用户的身份
     * @return List<Map<String,Object>>
     */
    @Select("SELECT\n" +
            "  u4.account,\n" +
            "  u4.USER_NAME,\n" +
            "  u4.USER_GROUP_ID,\n" +
            "  u4.ROLE_NAME,\n" +
            "  d.DEPT_NAME\n" +
            "FROM DENG_DEPT d RIGHT JOIN (SELECT\n" +
            "                               u3.*,\n" +
            "                               r1.ROLE_NAME\n" +
            "                             FROM DENG_ROLEINFO r1 RIGHT JOIN (SELECT\n" +
            "                                                                 u1.USER_ACCOUNT account,\n" +
            "                                                                 u1.USER_NAME,\n" +
            "                                                                 u1.EMAIL,\n" +
            "                                                                 u1.DEPT_ID,\n" +
            "                                                                 u2.USER_GROUP_ID\n" +
            "                                                               FROM DENG_USERINFO u1 LEFT JOIN DENG_USERINFO_ROLE u2\n" +
            "                                                                   ON u1.USER_ID = u2.USER_ID) u3\n" +
            "                                 ON r1.ROLE_ID = u3.USER_GROUP_ID) u4 ON u4.DEPT_ID = d.DEPT_ID")
    public List<Map<String,Object>> queryAllUserRole();

    /**
     * 查询所有角色
     * @return
     */
    @Select("select * from DENG_ROLEINFO")
    public List<Map<String,Object>> queryAllRole();

    /**
     * 为指定用户授权
     * @return int
     */
    @Insert("insert into DENG_USERINFO_ROLE(ID,USER_ID,USER_GROUP_ID) values(#{id},#{user_id},#{role_id})")
    public int addRole(Map<String,Object> map);

    /**
     * 通过用户名查用户ID
     * @return List<User>
     */
    @Select("select u1.USER_ID from DENG_USERINFO u1 WHERE u1.USER_NAME = #{user_name}")
    public List<Map<String,Object>> queryUidByUname(User user);

    /**
     * 查看一个人信息
     * @return
     */
    @Select("select d1.DEPT_NAME,u3.USER_ACCOUNT,u3.USER_PASSWORD,u3.PHONE,u3.EMAIL,u3.USER_NAME,u3.USER_NOTE from DENG_DEPT d1 RIGHT JOIN (select u2.*,r1.USER_GROUP_ID from DENG_USERINFO_ROLE r1 RIGHT JOIN (select * from DENG_USERINFO u1 where u1.USER_ACCOUNT = #{user_account}) u2 on r1.USER_ID = u2.USER_ID) u3 on u3.DEPT_ID = d1.DEPT_ID")
    public List<Map<String,Object>> queryOneInfo(User user);

    /**
     * 查询所有部门基本信息
     * @return
     */
    @Select("select d.DEPT_ID,d.DEPT_NAME,d.PARENT_ID,d.DEPT_DESC,u2.RENNUM from DENG_DEPT d INNER JOIN (select u1.DEPT_ID,count(u1.USER_NAME) RENNUM from DENG_USERINFO u1 GROUP BY u1.DEPT_ID) u2 ON u2.DEPT_ID=d.DEPT_ID")
    public List<Map<String,Object>> queryAllDept();

    /**
     * 获得权限的总数
     * @return
     */
    @Select("select count(1) NUM from DEND_OPERATION")
    public List<Map<String,Object>> getOperationTotal();
    /**
     * 查询所有权限信息及其持有者
     * @return
     */
    @Select("select o3.*,ROWNUM NUM from (SELECT\n" +
            "  o2.*,\n" +
            "  r2.ROLE_NAME\n" +
            "FROM DENG_ROLEINFO r2 RIGHT JOIN (SELECT\n" +
            "                                    o1.*,\n" +
            "                                    r1.USERGROUP_ID\n" +
            "                                  FROM DENG_ROLE_OPERATION r1 RIGHT JOIN (SELECT\n" +
            "                                                                            o.OPERATION_ID,\n" +
            "                                                                            o.OPERATION_NAME,\n" +
            "                                                                            o.PID,\n" +
            "                                                                            o.OPERATION_NOTE\n" +
            "                                                                          FROM DEND_OPERATION o) o1\n" +
            "                                      ON o1.OPERATION_ID = r1.OPERSTION_ID) o2 ON r2.ROLE_ID = o2.USERGROUP_ID order by r2.ROLE_NAME desc) o3")
    public List<Map<String,Object>> queryAllOperation();

    /**
     * 显示所有的角色
     * @return List<Map<String,Object>>
     */
    @Select("select * from DENG_ROLEINFO")
    public List<Map<String,Object>> showAllRole();

    /**
     * 查询还未配的权限
     * @return
     */
    @Select("select * from DEND_OPERATION o1 where  o1.STATE = 0")
    public List<Map<String,Object>> showAllNoOper();

    /**
     * 为角色添加权限
     * @param map
     * @return
     */
    @Insert("insert into DENG_ROLE_OPERATION(ID,USERGROUP_ID,OPERSTION_ID) VALUES(#{id},#{usergroup_id},#{operstion_id})")
    public int addOperation(Map map);

    /**
     * 更改权限状态
     * @param map
     * @return
     */
    @Update("UPDATE DEND_OPERATION o1 SET o1.NO = 1,PID = #{pid} where o1.OPERATION_ID = #{operation_id}")
    public int updateState(Map map);
}