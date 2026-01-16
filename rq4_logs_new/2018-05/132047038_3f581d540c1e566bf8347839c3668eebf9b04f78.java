package com.spitfire.forexmemo.dao;

import com.spitfire.forexmemo.domain.Administrator;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Created by H.W. on 5/3/2018.
 */
public interface AdministratorRepository extends MongoRepository<Administrator, String> {

    public Administrator save(Administrator administrator);

    public Administrator findByAid(String aid);

    public Administrator findByName(String name);

    public void deleteAdministratorByAid(String aid);
}