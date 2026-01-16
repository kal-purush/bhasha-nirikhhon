package com.capgemeni.railwayBooking.Dao;

import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Diff;
import org.javers.core.diff.changetype.ValueChange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.capgemeni.railwayBooking.Entity.Bookinginfo;
import com.capgemeni.railwayBooking.Repository.BookingRepository;

@Component
public class BookingDao {
	
	@Autowired
	BookingRepository BookinginfoRepo;
	
	private final MongoTemplate mongoTemplate;

    public BookingDao(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }
	
	
	public Bookinginfo addpassenger(Bookinginfo bookinginfo) {
		return BookinginfoRepo.save(bookinginfo);
	}
	public Bookinginfo bookinglist(String trainname,String username) 
	{
		Query query = new Query();
		query.addCriteria(Criteria.where("trainname").is(trainname));
		query.addCriteria(Criteria.where("username").is(username));
		Bookinginfo curretBookingInfo=mongoTemplate.findOne(query, Bookinginfo.class);
	    return curretBookingInfo;
	}
	public Bookinginfo updatepassenger(Bookinginfo bookinginfo) {
		Query query = new Query();
		query.addCriteria(Criteria.where("trainname").is(bookinginfo.getTrainname()));
		query.addCriteria(Criteria.where("username").is(bookinginfo.getUsername()));
		Bookinginfo curretBookingInfo=mongoTemplate.findOne(query, Bookinginfo.class);
		
		
		Javers javers = JaversBuilder.javers().build();
		Diff diff = javers.compare(curretBookingInfo, bookinginfo);
		Update update = new Update();
		for(int i=0;i<diff.getChanges().size();i++) {
			ValueChange change = diff.getChangesByType(ValueChange.class).get(i);
			update.set(change.getPropertyName(), change.getRight());
		}

		mongoTemplate.upsert(query, update, Bookinginfo.class);
		return mongoTemplate.findOne(query, Bookinginfo.class);
	}


}