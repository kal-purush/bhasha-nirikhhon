import SSM.bean.User;
import SSM.dao.UserMapperCache;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class CacheTest {

    public SqlSessionFactory getSqlSessionFactory(){
        String conf = "mybatis-config.xml";
        InputStream resourceAsStream = null;
        try {
            resourceAsStream = Resources.getResourceAsStream(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SqlSessionFactory build = new SqlSessionFactoryBuilder().build(resourceAsStream);

        return build;
    }

    /**
     * 一级缓存
     * 一级缓存的四种失效情况
     * 1，sqlSession不同，一级缓存在不同的sql会话间会失效
     * 2，sqlSession相同，在该数据每次增删改操作后会失效
     * 3，sqlSession相同，查询条件不同（当前一级缓存中还没有这个数据）
     * 4，sqlSession相同，手动清除了一级缓存（缓存清空SqlSession.clearCache()）
     */
    @Test
    public void test01(){

        SqlSessionFactory sqlSessionFactory = getSqlSessionFactory();
        SqlSession sqlSession1 = sqlSessionFactory.openSession();
        SqlSession sqlSession2 = sqlSessionFactory.openSession();
        //第一次查询
        UserMapperCache mapper = sqlSession1.getMapper(UserMapperCache.class);
        User user = mapper.selectUserById(1);

        //第二次查询前执行修改操作，一级缓存失效
        /*Boolean aBoolean = mapper.updateUserByUser(user);
        System.out.println(aBoolean);*/
        //第二次查询
        UserMapperCache mapper1 = sqlSession1.getMapper(UserMapperCache.class);
        User user1 = mapper1.selectUserById(1);
        //使用同一个sql会话，结果只发送了一条查询sql（Preparing: select *from mybatis where id = ? ）
        System.out.println(user);
        user.setUserName("house");
        System.out.println(user1);
        sqlSession1.close();
        sqlSession2.close();
    }

    /**
     * 二级缓存(全局缓存)，基于namespace级别的缓存
     * 二级缓存不同于一级缓存，一级缓存默认开启，二级缓存默认关闭
     *
     * 效果：数据会从二级缓存中获取
     * 查出的数据都会被默认先放在一级缓存中，
     * 只有会话提交或者关闭以后，一级缓存中的数据才会转移到二级缓存中。
     * 使用：
     * 1）开启全局二级缓存设置<setting name="cacheEnabled" value="true"></>
     * 2)  去mapper.xml中配置使用二级缓存
     * <cache></cache>
     * 3）我们的pojo实现序列化接口
     */
    @Test
    public void test02(){
        SqlSessionFactory sqlSessionFactory = getSqlSessionFactory();
        SqlSession sqlSession1 = sqlSessionFactory.openSession();
        SqlSession sqlSession2 = sqlSessionFactory.openSession();
        UserMapperCache mapper = sqlSession1.getMapper(UserMapperCache.class);
        UserMapperCache mapper1 = sqlSession2.getMapper(UserMapperCache.class);
        User user = mapper.selectUserById(1);

        sqlSession1.close();
        User user1 = mapper1.selectUserById(1);
        System.out.println(user);
        System.out.println(user1);
        sqlSession2.close();

    }
}
