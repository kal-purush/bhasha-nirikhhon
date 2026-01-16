package com.nice.test;

import com.nice.domain.Menu;
import com.nice.service.MenuService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class AddMenuTest {
    @Autowired
    private MenuService menuService;

    @Test
    public void test(){
        Menu menu = new Menu();
        menu.setChannel("/nice");
        menu.setIcon("nice");
        menu.setLocked(true);
        menu.setMenuCreateDate(new Date());
        menu.setMenuName("nice");
        menu.setMenuUpdateDate(new Date());
        menu.setNLevel(1L);
        menu.setOrdno(1L);
        menu.setParam("nice");
        menu.setPaterId(1L);
        menu.setRemark("nice");
        menu.setScort(1L);
        menuService.addMenu(menu);
    }
}