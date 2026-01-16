package com.zhang.libweb.mapper.book;

import com.zhang.libweb.model.BookClassDTO;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface BookClassMapper {

    /**
     * 根据名称模糊查询全部分类信息
     * @param name 分类名称
     * @return 匹配的数据集
     */
    List<BookClassDTO> findListByName(String name);

    /**
     * 删除
     * @param id 数据主键
     * @return
     */
    int deleteByPrimaryKey(Integer id);

    /**
     * 新增
     * @param record 实体类（不好含ID）
     * @return
     */
    int insert(BookClassDTO record);

    /**
     *     selective区别在 如果属性为空则对应属性不更新
     * @param record 实体类（不好含ID）
     * @return
     */
    int insertSelective(BookClassDTO record);

    BookClassDTO selectByPrimaryKey(Integer id);

    /**
     * selective更新
     * @param record 实体类（不好含ID）
     * @return
     */
    int updateByPrimaryKeySelective(BookClassDTO record);

    /**
     * 更新
     * @param record 实体类（不好含ID）
     * @return
     */
    int updateByPrimaryKey(BookClassDTO record);
}