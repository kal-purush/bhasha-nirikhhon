package com.xiaojihua.test;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class OracleTest {
    private String driver = "oracle.jdbc.driver.OracleDriver";
    private String url = "jdbc:oracle:thin:@192.168.237.128:1521:orcl";
    private String user = "scott";
    private String password = "tiger";
    private Connection connection;
    private Statement statement;
    private ResultSet rs;
    private CallableStatement cs;

    @Before
    public void init() throws ClassNotFoundException, SQLException {
        //1、加载驱动
        Class.forName(driver);
        //2、获取链接
        connection = DriverManager.getConnection(url, user, password);
        //3、获取sql执行对象
        statement = connection.createStatement();
    }

    @After
    public void closeAll() throws SQLException {
        if(rs != null){
            rs.close();
        }

        if(statement != null){
            statement.close();
        }

        if(connection != null){
            connection.close();
        }

    }

    //链接oracle数据库查询员工信息
    //普通方式
    @Test
    public void testConnection() throws ClassNotFoundException, SQLException {

        /*//1、加载驱动
        Class.forName(driver);
        //2、获取链接
        Connection connection = DriverManager.getConnection(url, user, password);
        //3、获取sql执行对象
        Statement statement = connection.createStatement();*/
        //4、执行sql语句，并且获取结果集(只有查询有结果集,其他都是int返回值)
        rs = statement.executeQuery("select * from emp");
        //5、处理结果集
        while(rs.next()){
            System.out.println("员工编号:" + rs.getString(1) + ",员工姓名:" + rs.getString("ename"));
        }
        //6、关闭链接
        /*rs.close();;
        statement.close();
        connection.close();*/
    }

    /**
     * 通过prepareCall方式，执行sql语句并且处理结果集
     * prepareCall不仅可以执行普通的sql语句，也可以执行存储过程和函数
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void testPrepareCall() throws ClassNotFoundException, SQLException {

        /*//1、加载驱动
        Class.forName(driver);
        //2、获取链接
        Connection connection = DriverManager.getConnection(url,user,password);*/
        //3、获取prepareCall，并执行sql语句
        cs = connection.prepareCall("select * from emp");
        //4、处理结果集
        rs = cs.executeQuery();
        while(rs.next()){
            System.out.println("员工姓名：" + rs.getString(1) + ",员工编号：" + rs.getString("empno"));
        }
        //5、关闭链接
        /*rs.close();
        cs.close();
        connection.close();*/
    }

    /**
     * 调用pro_add_sal存储函数
     */
    @Test
    public void testCallProcedureAddSal() throws SQLException {
        cs = connection.prepareCall("{call pro_add_sal(?)}");
        cs.setInt(1,7499);
        cs.execute();//没有返回值直接执行一次就行
    }

    /**
     * 调用pro_emp_list存储过程，包含输入和输出函数
     * @throws SQLException
     */
    @Test
    public void testCallProcedureEmpList() throws SQLException {
        cs = connection.prepareCall("{call pro_emp_list(?,?)}");
        cs.setInt(1,10);
        /**
         * 注册输出函数，这里注册的是一个游标类型
         * 如果是其他的String或者是int可以使用
         * OracleTypes的其他类型
         */
        cs.registerOutParameter(2,OracleTypes.CURSOR);
        cs.execute();
        /**
         * 获取输出参数。
         * 这里需要获取Cursor类型的，需要将CallableStatement强转为OracleCallableStatement
         * 其他类型的视情况而定，但是都是通过cs.get...的方法来获取输出
         */
        rs = ((OracleCallableStatement) cs).getCursor(2);
        while(rs.next()){
            System.out.println("员工编号：" + rs.getString(1) + ",员工名称：" + rs.getString("ename"));
        }
    }

    /**
     * 调用存储方法
     * @throws SQLException
     */
    @Test
    public void testCallFunctionFunEmpDname() throws SQLException {
        //调用存储函数需要使用{}
        cs = connection.prepareCall("{? = call fun_emp_dname(?)}");
        //注册输出参数，也就是返回值
        cs.registerOutParameter(1,OracleTypes.VARCHAR);
        //设置输入参数
        cs.setInt(2,10);
        cs.execute();
        //获取输出参数
        String dname = cs.getString(1);
        System.out.println(dname);
    }
}