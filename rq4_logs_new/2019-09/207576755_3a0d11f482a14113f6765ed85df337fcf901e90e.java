package com.itheima.dao;

import com.itheima.domain.ContactInfo;

import java.util.List;

// 通讯录相关DAO
public interface ContactInfoDAO {
    /**
     * 查询所有通讯录记录
     * @return 所有通讯录记录
     */
//    public List<ContactInfo> queryAll();

    /**
     * 删除指定的通讯录
     * @param id 通过通讯录的id进行删除
     * @return 此条sql语句，受影响的记录有几条
     */
//    public int delContact(String id);

    /**
     * 添加通讯录
     * @param contact 通讯录JavaBean
     * @return 受影响的记录有几条
     */
//    public int addContact(ContactInfo contact);

    /**
     * 分页查询通讯录记录
     * @param pageOffset limit第1个关键字
     * @param pageSize   limit第2个关键字
     * @return 分页查询的结果
     */
//    public List<ContactInfo> queryPage(int pageOffset, int pageSize);

    /**
     * 统计有多少条记录
     * @return 记录的条数
     */
    public int queryRecordCount();

    /**
     * 根据唯一标识查询通讯录
     * @param id 唯一标识
     * @return 查询所得到的结果
     */
    public List<ContactInfo> queryContactById(String id);

    /**
     * 修改通讯录记录
     * @param contact 通讯录记录的实体
     * @return 修改所造成的影响的记录有几条
     */
    public int modifyContact(ContactInfo contact);

    public List<ContactInfo> queryAll();

    public int delContact(String id);

    public int addContact(ContactInfo contactInfo);

    List<ContactInfo> queryPage(int pageOff, int pageSize);
}