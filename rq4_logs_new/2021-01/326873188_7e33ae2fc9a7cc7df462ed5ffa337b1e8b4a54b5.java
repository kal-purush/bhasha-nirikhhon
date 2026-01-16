package com.fly.data.sync.constant;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/8
 */
public interface SyncSql {
    String INSERT_SQL = "insert into ${tempTable} (${fieldList}) values (${propertyList})";

    String QUERY_ADD_SQL = "select ${a.fieldList} from ${tempTable} a " +
            "left join ${table} b on a.${id} = b.${id} " +
            "where b.${id} is null";

    String ADD_SQL = "insert into ${table} (${fieldList}) " +
            "select ${a.fieldList} from ${tempTable} a " +
            "left join ${table} b on a.${id} = b.${id} " +
            "where b.${id} is null";

    String QUERY_UPDATE_SQL = "select ${a.fieldList} from ${tempTable} a " +
            "left join ${table} b on a.${id} = b.${id} " +
            "where a.${updateTime} > b.${updateTime}";

    String QUERY_OLD_SQL = "select ${b.fieldList} from ${tempTable} a " +
            "left join ${table} b on a.${id} = b.${id} " +
            "where a.${updateTime} > b.${updateTime}";

    String UPDATE_SQL = "update ${table}, ${tempTable} " +
            "set ${updateField} " +
            "where ${table}.${id} = ${tempTable}.${id} " +
            "and ${table}.${updateTime} < ${tempTable}.${updateTime}";

    String QUERY_DELETE_SQL = "select ${a.fieldList} from ${table} a " +
            "left join ${tempTable} b on a.${id} = b.${id} " +
            "where b.${id} is null";

    String DELETE_SQL = "delete a from ${table} a " +
            "left join ${tempTable} b on a.${id} = b.${id} " +
            "where b.${id} is null";
}