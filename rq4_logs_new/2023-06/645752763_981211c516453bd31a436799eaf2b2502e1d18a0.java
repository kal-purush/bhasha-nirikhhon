package com.ksumit.login.DataBase;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.ksumit.login.DataBaseImplement.UserInforamation;


public interface DataBase extends JpaRepository<UserInforamation, Long> {
    
    @Query(value = "select * from detail a where a.email=?1 and a.password=?2",nativeQuery = true)
    public UserInforamation getresult(String email,String password);

}