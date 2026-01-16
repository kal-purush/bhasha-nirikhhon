package dao;

import entity.Seckill;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface SeckillDao {
    //对商品的操作

    /*接口1:减库存
     * 返回值为减去的数量（影响行数）*/
    int reduceNumber(@Param("seckillId")Long seckillId, @Param("killTime")Date killTime);//减库存的id，执行减库存的时间

    //接口2：查询需求(根据ID查询秒杀对象)
    Seckill queryById(Long seckillId);

    //接口3：做一个列表(根据偏移量查询秒杀商品)
    List<Seckill> queryAll(@Param("offset") int offset, @Param("limit")int limit);

    void killByProcedure(Map<String,Object> paramMap);
}