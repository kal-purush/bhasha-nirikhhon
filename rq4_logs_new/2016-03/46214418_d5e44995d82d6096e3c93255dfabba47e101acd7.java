package com.sunzequn.sunmysql.connection;

import com.sunzequn.sunmysql.exception.ConfigException;
import com.sunzequn.sunmysql.utils.PropertiesUtil;
import com.sunzequn.sunmysql.utils.StringUtil;

/**
 * Created by Sloriac on 16/3/6.
 * <p>
 * The configuration information for your project.
 */
public class Configuration {

    /**
     * The url of JDBC.
     */
    public static String jdbcUrl;
    /**
     * The user name of the database.
     */
    public static String user;
    /**
     * The password for this user.
     */
    public static String password;
    /**
     * The number of connections to MySQL.
     * If you don`t have this parameter, we will use the default value 10.
     */
    public static int initialPoolSize = 10;
    /**
     * The package for javabean classes imported from tables.
     * If you don`t use this function, ignore it.
     */
    public static String beanPackage;

    /**
     * static block to load configuration.
     */
    static {
        jdbcUrl = PropertiesUtil.getValue("jdbcUrl");
        checkParameter("jdbcUrl", jdbcUrl);
        user = PropertiesUtil.getValue("user");
        checkParameter("user", user);
        password = PropertiesUtil.getValue("password");
        checkParameter("password", password);
        String size = PropertiesUtil.getValue("initialPoolSize");
        if (StringUtil.isNotEmpty(size)) {
            initialPoolSize = Integer.valueOf(size);
        }
        String bean = PropertiesUtil.getValue("beanPackage");
        if (StringUtil.isNotEmpty(bean)) {
            beanPackage = bean;
        }
    }

    /**
     * Determine whether the parameter is defined in the file conf.properties.
     *
     * @param parameter The parameter which needs checking.
     */
    public static void checkParameter(String parameter, String value) {
        if (StringUtil.isEmpty(value)) {
            try {
                throw new ConfigException("The parameter:" + parameter + " is not defined.");
            } catch (ConfigException e) {
                e.printStackTrace();
            }
        }
    }
}