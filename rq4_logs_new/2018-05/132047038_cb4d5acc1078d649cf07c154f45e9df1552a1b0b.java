package com.spitfire.forexmemo.dao;

import com.spitfire.forexmemo.domain.NewsPiece;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.transaction.annotation.Transactional;

import java.text.SimpleDateFormat;
import java.util.Date;


public class PartialUpdateImpl implements PartialUpdate {

    @Autowired
    MongoTemplate mongoTemplate;

    @Transactional
    public String partialUpdate(String Id, String updateContent, String type){
        if(type.equals("_id")){
            return "No no no";
        }
        if(!type.equals("postTime") && !type.equals("actualTime")){
            Query query = new Query();
            query.addCriteria(Criteria.where("_id").is(Id));
            Update update = new Update();
            update.set(type, updateContent);
            mongoTemplate.updateFirst(query, update, NewsPiece.class);
        } else {
            SimpleDateFormat s1 = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
            try{
                Date newDate = s1.parse(updateContent);
                Query query = new Query();
                query.addCriteria(Criteria.where("_id").is(Id));
                Update update = new Update();
                update.set(type, newDate);
                mongoTemplate.updateFirst(query, update, NewsPiece.class);
            } catch (Exception e){
                return "Update Date Fail!";
            }
        }
        return "Update Success!";
    }
}