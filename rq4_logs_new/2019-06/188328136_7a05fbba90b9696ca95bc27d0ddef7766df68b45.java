package com.macoredroid.onlinebookstore.daoimpl;

import com.macoredroid.onlinebookstore.dao.AdminDao;
import com.macoredroid.onlinebookstore.entity.Admin;
import com.macoredroid.onlinebookstore.repository.AdminRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class AdminDaoimpl  implements AdminDao {
    @Autowired
    private AdminRepository  AdminRepository;
    @Override
    public Admin findOne(String username) {
        return AdminRepository.findByUsername(username);
    }

    @Override
    public void save(String username, String password, String email) {
        Admin tempAdmin = new Admin(username,password,email);
        AdminRepository.save(tempAdmin);
    }

    @Override
    public void save(Admin Admin) {
        AdminRepository.save(Admin);
    }

    @Override
    public List<Admin> findAll() {
        return AdminRepository.findAll();
    }

    @Override
    public void remove(Integer id) {
        AdminRepository.deleteById(id);
    }
}