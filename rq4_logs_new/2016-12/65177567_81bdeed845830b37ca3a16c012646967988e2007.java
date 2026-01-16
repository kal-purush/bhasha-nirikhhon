package com.cyxd.bog.base.dao;


public interface BaseDao<T> {

//    @InsertProvider(type = SqlProvider.class, method = "insert")
//    @Options(useGeneratedKeys = true)
    public int insert(T bean);

//    @DeleteProvider(type = SqlProvider.class, method = "delete")
    public int delete(T bean);

//    @UpdateProvider(type = SqlProvider.class, method = "update")
    public int update(T bean);

//    @SelectProvider(type = SqlProvider.class, method = "findFirst")
    public T findFirst(T bean);
}