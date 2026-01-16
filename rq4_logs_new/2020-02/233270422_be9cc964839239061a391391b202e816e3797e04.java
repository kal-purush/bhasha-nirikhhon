package com.vcode.Impl;

import com.vcode.dao.ProblemDao;
import com.vcode.entity.Problem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class ProblemDaoImpl implements ProblemDao {

  @Autowired
  private MongoTemplate mongoTemplate;

  @Override
  public void saveProblem(Problem problem) {
    mongoTemplate.save(problem);
  }

  @Override
  public Problem findByOriginId(String originId) {
    Query query = new Query(Criteria.where("origin_id").is(originId));
    return mongoTemplate.findOne(query, Problem.class);
  }

  @Override
  public void updateProblem(Problem problem) {
    // 只能编辑非爬虫获取的的题目
    if (problem.getOrigin().equals("vcode")) {
      Query query = new Query(Criteria.where("id").is(problem.getId()));
      Update update = new Update().set("nickname", problem.getUpdateData());
      mongoTemplate.updateFirst(query, update, Problem.class);
    }
  }

  @Override
  public void DeleteProblemByOriginId(String originId) {
    Query query = new Query(Criteria.where("origin_id").is(originId));
    mongoTemplate.remove(query, Problem.class);
  }
}