package com.vcode.Impl;

import com.vcode.dao.TagDao;
import com.vcode.entity.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class TagDaoImpl implements TagDao {

  @Autowired
  private MongoTemplate mongoTemplate;

  @Override
  public void saveTag(Tag tag) {
    mongoTemplate.save(tag);
  }

  @Override
  public Tag findUserByUserName(String name) {
    Query query = new Query(Criteria.where("name").is(name));
    return mongoTemplate.findOne(query,Tag.class);
  }

  @Override
  public void updateTag(Tag tag) {
    Query query = new Query(Criteria.where("name").is(tag.getName()));
    Update update = new Update().set("problems",tag.getProblems());
    mongoTemplate.updateFirst(query, update, Tag.class);
  }

  @Override
  public void deleteTagByName(String name) {
    Query query = new Query(Criteria.where("name").is(name));
    mongoTemplate.remove(query,Tag.class);
  }
}