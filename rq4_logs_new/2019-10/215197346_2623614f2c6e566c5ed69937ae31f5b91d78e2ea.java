package cn.itheima.service;

import cn.itheima.dao.UserMapper;
import cn.itheima.pojo.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
public class UserServiceImpl implements UserService {
    @Autowired
    private UserMapper userMapper;

    public List<User> haha() {
        return userMapper.findAllUser();
    }

    @Override
    public void add(User user) {
        userMapper.add(user);
    }

    @Override
    public void del(Integer id) {
        userMapper.del(id);
    }

    @Override
    public User edit(Integer id) {
        return userMapper.edit(id);
    }

    @Override
    public void doEdit(User user) {
        userMapper.doEdit(user);
    }
}