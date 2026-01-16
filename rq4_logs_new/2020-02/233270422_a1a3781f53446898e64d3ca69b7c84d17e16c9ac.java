package com.vcode.Impl;

import com.vcode.dao.AboutDao;
import com.vcode.entity.About;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class AboutDaoImpl implements AboutDao {

    @Autowired
    MongoTemplate mongoTemplate;

    @Override
    public void updateAbout(About about) {
        ObjectId id=getAbout().getId();
        Query query = new Query(Criteria.where("id").is(id));
        Update update = new Update().set("doc", about.getDoc());
        mongoTemplate.updateFirst(query, update, About.class);
    }

    @Override
    public About getAbout() {
        Query query = new Query();
        return mongoTemplate.findOne(query, About.class);
    }

    @Override
    public boolean isExist() {
        About a = getAbout();
        return a.getDoc() != null;
    }
}