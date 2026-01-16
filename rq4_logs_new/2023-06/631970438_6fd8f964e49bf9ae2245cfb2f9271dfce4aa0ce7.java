package com.mujio.dal.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.mujio.dal.entity.SysDatasource;
import com.mujio.dal.entity.SysObjField;
import com.mujio.dal.entity.SysObject;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.teasoft.bee.osql.ObjToSQL;
import org.teasoft.bee.osql.api.PreparedSql;
import org.teasoft.bee.osql.api.SuidRich;
import org.teasoft.honey.osql.core.BeeFactory;
import org.teasoft.honey.osql.core.BeeFactoryHelper;
import org.teasoft.honey.osql.core.Logger;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Driver;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 用于获取指定数据源
 */
@Component
public class DataSourceConfig {

    @Resource
    DataSource defaultDataSource;

    private static Map<String, DataSource> dataSourceMap = new LinkedHashMap<>();

    /**
     * 获取通用 Suid
     *
     * @param tenantId       租户ID
     * @param dataSourceName 数据源名称
     * @param dataSourceType 数据源类型
     * @return suidRich
     */
    public SuidRich getSuid(int tenantId, String dataSourceName, String dataSourceType) {

        String cacheKey = getCacheKey(tenantId, dataSourceType, dataSourceName);
        if (cacheKey.equals(getCacheKey(0, "mysql", "mujio"))) {
            return getSystemSuid();
        }

        // 如果没有缓存则查询并将对应数据源加入缓存
        if (!dataSourceMap.containsKey(cacheKey)) {
            SysDatasource sysDatasource = new SysDatasource();
            sysDatasource.setTenantId(tenantId);
            sysDatasource.setUserName(dataSourceName);
            sysDatasource.setType(dataSourceType);
            DataSource needCacheSource = getDataSource(sysDatasource);
            // 加入缓存
            dataSourceMap.put(cacheKey, needCacheSource);
            // 保存数据源
            BeeFactory.getInstance().setDataSourceMap(dataSourceMap);
        }

        // 切换到 cacheKey 对应的数据源
        SuidRich suidRich = BeeFactory.getHoneyFactory().getSuidRich();
        suidRich.setDataSourceName(cacheKey);
        return suidRich;
    }

    /**
     * 根据数据源信息获取 DataSource 对象
     *
     * @param sysDatasource
     * @return
     */
    private DataSource getDataSource(SysDatasource sysDatasource) {
        BeeFactory.getInstance().setDataSource(defaultDataSource);
        SuidRich suidRich = BeeFactory.getHoneyFactory().getSuidRich();
        SysDatasource sysDatasourceRecord = suidRich.selectOne(sysDatasource);
        Assert.notNull(sysDatasourceRecord, String.format("未查询到数据源，{%s}", sysDatasource));

        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(sysDatasourceRecord.getUrl());
        dataSource.setUsername(sysDatasourceRecord.getUserName());
        dataSource.setPassword(sysDatasourceRecord.getPassword());
        dataSource.setDbType(sysDatasourceRecord.getType());
        try {
            Driver d = (Driver) Class.forName("com.mysql.jdbc.Driver").newInstance();
            dataSource.setDriver(d);
            dataSource.init();
        } catch (Exception e) {
            Logger.error(String.format("获取数据源连接失败, dataSource:{%s}", sysDatasourceRecord));
            throw new RuntimeException(e);
        }
        return dataSource;
    }

    public SuidRich getSystemSuid() {
        BeeFactory.getInstance().setDataSource(defaultDataSource);
        return BeeFactory.getHoneyFactory().getSuidRich();
    }

    public SuidRich getSystemSuid1() {
        BeeFactory.getInstance().setDataSource(defaultDataSource);
        return BeeFactory.getHoneyFactory().getSuidRich();
    }

//    public PreparedSql getPreparedSql() {
//        BeeFactory.getInstance().setDataSource(defaultDataSource);
//        String sql = BeeFactory.getHoneyFactory().getObjToSQL().toSelectSQL(entity, condition);
//        return BeeFactory.getHoneyFactory().getBeeSql().select;
//    }

    private static String getCacheKey(int tenantId, String dataSourceType, String datasourceName) {
        return String.format("ds_%s_%s_%s", tenantId, dataSourceType, datasourceName);
    }


    public Map<String, Object> getCommonEmptyObj(int objId) {
        SuidRich systemSuid = getSystemSuid();
        SysObject sysObject = systemSuid.selectById(SysObject.class, objId);
        SysObjField sysObjField = new SysObjField();
        sysObjField.setObjId(objId);
        List<SysObjField> fieldList = systemSuid.select(sysObjField);

        HashMap<String, Object> res = new HashMap<>();
        res.put("obj", sysObject);
        res.put("field", fieldList);

        return res;
    }
}