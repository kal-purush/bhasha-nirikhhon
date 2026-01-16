package com.project.menu.dao.impl;

import com.project.menu.dao.MenuDao;
import com.project.menu.vo.MenuVo;
import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository("menuDao")
public class MenuDaoImpl implements MenuDao {

    @Autowired
    private SqlSession sqlSession;

    @Override
    public List<MenuVo> getMenuList() {
        List<MenuVo> menuList = sqlSession.selectList("Menu.MenuList");
        return menuList;
    }

}