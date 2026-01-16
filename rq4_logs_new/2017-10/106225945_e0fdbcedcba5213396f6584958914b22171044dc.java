package com.backpack.backpacktours.Repository;

import com.backpack.backpacktours.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;

@Transactional
@Repository
@EnableTransactionManagement
public class RegistrationRepository {

    @PersistenceContext
    private EntityManager entityManager;



    public String postuserData(User user){
        try {
            entityManager.persist(user);
            return "SUCCESS";
        }
        catch (Exception e){
            System.out.println("user = [" + e.toString() + "]");
            return "FAILED";
        }
    }
}