package hello;

import hello.domain.App;
import hello.domain.AppDao;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Created by poolebu on 1/23/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class AppDaoTests {

    @Autowired
      private MockMvc mockMvc;

    @Autowired
        private AppDao appDao;

    @Test
    public void findAllAppsTest(){


        App app = appDao.findOne(1);

        Assert.assertTrue(app!= null);


    }


}