package com.upgrad.quora.service.business;


import com.upgrad.quora.service.dao.AdminDao;
import com.upgrad.quora.service.entity.UsersEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AdminBusinessService {

    @Autowired
    private AdminDao adminDao;

    @Transactional(propagation = Propagation.REQUIRED)
    public UsersEntity deleteUser(final String uuid){


        UsersEntity user = adminDao.deleteUser(uuid);

        return user;
    }



}