package com.fly.data.sync.dao.impl;

import com.fly.data.sync.dao.ModelDao;
import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.entity.UpdateData;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/7
 */
@Repository
public class ModelDaoImpl implements ModelDao {

    private final JdbcTemplate jdbcTemplate;

    private final NamedParameterJdbcTemplate namedJdbcTemplate;


    public ModelDaoImpl(JdbcTemplate jdbcTemplate, NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.namedJdbcTemplate = namedParameterJdbcTemplate;
    }

    @Override
    public <T> void loadToTemp(List<T> dataList, DataModel<T> model) {

        String insertSql = "";

        namedJdbcTemplate.batchUpdate(insertSql, SqlParameterSourceUtils.createBatch(dataList));

    }

    @Override
    public <T> List<T> add(DataModel<T> model) {
        //b差a
        String queryAddSql = "select ${a.fieldList} from ${tempTable} a " +
                "left join ${table} b on a.${id} = b.${id} " +
                "where b.${id} is null";

        String addSql = "insert into ${table} (${a.fieldList}) " +
                "select ${a.fieldList} from ${tempTable} a " +
                "left join ${table} b on a.${id} = b.${id} " +
                "where b.${id} is null";


        List<T> addList = jdbcTemplate.query(queryAddSql, model.getRowMapper());
        jdbcTemplate.update(addSql);

        return addList;
    }

    @Override
    public <T> UpdateData<T> update(DataModel<T> model) {
        return null;
    }


    @Override
    public <T> List<T> delete(DataModel<T> model) {
        String queryDeleteSql = "select ${a.fieldList} from ${table} a " +
                "left join ${tempTable} b on a.${id} = b.${id} " +
                "where b.${id} is null";

        String deleteSql = "delete a from ${table} a " +
                "left join ${tempTable} b on a.${id} = b.${id} " +
                "where b.${id} is null";


        queryDeleteSql = parseSql(queryDeleteSql, model);

        deleteSql = parseSql(deleteSql, model);

        List<T> deleteList = jdbcTemplate.queryForList(queryDeleteSql, model.getModelClass());

        jdbcTemplate.update(deleteSql);

        return deleteList;
    }


    @Override
    public <T> void truncateTemp(DataModel<T> model) {
        jdbcTemplate.execute("truncate " + model.getTempTable());
    }

    private <T> String parseSql(String sql, DataModel<T> model) {
        return sql.replace("${table}", model.getTable())
                .replace("${tempTable}", model.getTempTable())
                .replace("${id}", model.getId())
                .replace("${fieldList}", model.getFieldNameListString())
                .replace("${a.fieldList}", model.getFieldNameListStringWithPrefix("a"))
                .replace("${b.fieldList}", model.getFieldNameListStringWithPrefix("b"));
    }
}