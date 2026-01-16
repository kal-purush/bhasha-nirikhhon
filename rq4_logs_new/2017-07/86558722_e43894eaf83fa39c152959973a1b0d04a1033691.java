package mybatisgen;

import cn.springBoot.MyBootApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created by Administrator on 2017/07/20.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootConfiguration
@SpringBootTest(classes={MyBootApplication.class})
@Transactional(timeout = 2,transactionManager = "txTransactionManager")
public class TransTimeOutTestService {

    @Autowired
    private SqlSessionTemplate sqlSessionTemplate;

    @Test
    public  void test1()throws  Exception{

        System.out.println(System.currentTimeMillis());
        sqlSessionTemplate.getConnection().createStatement().execute("select * from my_user_t");
        Thread.sleep(5000L);
        sqlSessionTemplate.getConnection().createStatement().execute("select * from my_user_t");
        System.out.println("############update timeout");
        System.out.println(System.currentTimeMillis());

        System.out.println("########超时后执行");

    }
}