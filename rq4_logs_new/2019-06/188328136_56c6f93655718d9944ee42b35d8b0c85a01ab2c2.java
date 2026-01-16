package com.macoredroid.onlinebookstore.serviceimpl;

import com.macoredroid.onlinebookstore.dao.UserDao;
import com.macoredroid.onlinebookstore.entity.User;
import com.macoredroid.onlinebookstore.service.ChangeUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ChangeUserServiceimpl implements ChangeUserService {
    @Autowired
    UserDao UserDao;
    @Override
    public boolean ChangeUsername(String username, String newusername) {
        User tempUser= UserDao.findOne(username);
        tempUser.setUsername(newusername);
        UserDao.save(tempUser);
        return true;
    }

    @Override
    public boolean ChangeEmail(String username, String newemail) {
        User tempUser= UserDao.findOne(username);
        if(tempUser==null)
        {
            return false;
        }
        tempUser.setEmail(newemail);
        UserDao.save(tempUser);
        return true;
    }
}