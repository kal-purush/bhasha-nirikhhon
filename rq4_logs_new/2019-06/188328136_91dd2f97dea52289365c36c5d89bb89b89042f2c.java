package com.macoredroid.onlinebookstore.serviceimpl;

import com.macoredroid.onlinebookstore.dao.UserDao;
import com.macoredroid.onlinebookstore.entity.User;
import com.macoredroid.onlinebookstore.service.BlockUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BlockUserServiceimpl implements BlockUserService {
    @Autowired
    UserDao UserDao;
    @Override
    public boolean BlockUserService(String username)
    {
        User tempUser= UserDao.findOne(username);
        if(tempUser==null)
        {
            return false;
        }
        else
        {
            tempUser.setStar(-1);
            UserDao.save(tempUser);
            return true;
        }
    }

    @Override
    public boolean UnBlockUserService(String username) {
        User tempUser= UserDao.findOne(username);
        if(tempUser==null)
        {
            return false;
        }
        else
        {
            tempUser.setStar(2);
            UserDao.save(tempUser);
            return true;
        }
    }
}