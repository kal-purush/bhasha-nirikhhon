package com.webSpider.dao;

import com.webSpider.pojo.CleComposerInfo;
import org.springframework.stereotype.Repository;

@Repository
public interface CleComposerInfoMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(CleComposerInfo record);

    int insertSelective(CleComposerInfo record);

    CleComposerInfo selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(CleComposerInfo record);

    int updateByPrimaryKey(CleComposerInfo record);
}