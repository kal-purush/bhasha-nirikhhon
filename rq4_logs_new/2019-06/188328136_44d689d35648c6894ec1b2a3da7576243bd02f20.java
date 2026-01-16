package com.macoredroid.onlinebookstore.serviceimpl;

import com.macoredroid.onlinebookstore.dao.UserDao;
import com.macoredroid.onlinebookstore.entity.User;
import com.macoredroid.onlinebookstore.service.UnsubscribeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UnsubscribeServiceimpl implements UnsubscribeService {
    @Autowired
    private UserDao UserDao;
    @Override
    public boolean Unsubscribe(String Username) {
        User tempUser=UserDao.findOne(Username);
        if(tempUser==null) {
            return false;
        }
        else
        {
            UserDao.remove(tempUser.getUserID());
            return true;
        }
    }
}