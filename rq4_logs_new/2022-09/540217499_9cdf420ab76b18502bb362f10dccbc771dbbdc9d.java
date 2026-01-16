package com.quietlip.carol_shop;

import com.quietlip.carol_shop.model.Inventory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

@SpringBootApplication
public class CarolShopApplication {

    public static void main(String[] args) {
        SpringApplication.run(CarolShopApplication.class, args);
    }

//    @Bean
//    CommandLineRunner runner(InventoryRepo repo, MongoTemplate mongoTemplate){
//        return args -> {
//            Pots.js inventory = new Pots.js(
//                    "Curtis Stone", false, 10.00d
//            );
//            Pots.js inventory1 = new Pots.js(
//                    "Curtis Stone", false, 10.00d
//            );
//            Query query = new Query();
//            query.addCriteria(Criteria.where("brand").is("Curtis Stone"));
//            List<Pots.js> inventoryList = mongoTemplate.find(query, Pots.js.class);
//            if(inventoryList.size() > 1){
//                throw new IllegalStateException("too many items of the same brand");
//            }
//            if(inventoryList.isEmpty()){
//                System.out.println("inserting item");
//                repo.insert(inventory1);
//            } else {
//                System.out.println("item already in inventory");
//            }
//
//        };
//    }

}