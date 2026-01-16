package me.zonglun.seckilling.dao;

import me.zonglun.seckilling.domain.MiaoshaUser;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface MiaoshaUserDao {
	
	@Select("select * from user where id = #{id}")
	public MiaoshaUser getById(@Param("id")long id);

	@Update("update user set password = #{password} where id = #{id}")
	public void update(MiaoshaUser toBeUpdate);
}