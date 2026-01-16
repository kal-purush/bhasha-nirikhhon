package com.vcode.Impl;

import com.vcode.dao.ContestDao;
import com.vcode.entity.Contest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

public class ContestDapImpl implements ContestDao {

  @Autowired
  private MongoTemplate mongoTemplate;

  @Override
  public void saveContest(Contest contest) {
    mongoTemplate.save(contest);
  }

  @Override
  public Contest findByName(String Name) {
    Query query = new Query(Criteria.where("name").is(Name));
    return mongoTemplate.findOne(query, Contest.class);
  }

  @Override
  public void updateContest(Contest contest) {
    Query query = new Query(Criteria.where("id").is(contest.getId()));
    Update update = new Update().push("$set", contest.getUpdateData());
    mongoTemplate.updateFirst(query, update, Contest.class);
  }

  @Override
  public void DeleteContestByName(String Name) {
    Query query = new Query(Criteria.where("name").is(Name));
    mongoTemplate.remove(query, Contest.class);
  }
}