package com.weddingnotification;

import com.palipind.weddingdb.DeviceToken;
import com.palipind.weddingdb.Event;
import com.palipind.weddingdb.WeddingEvent;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;

import java.util.ArrayList;
import java.util.List;

import java.util.Date;


/**
 * @author Neel Jain
 */

public class DbAccessor {
	
	private Session dbConnection;

	public DbAccessor(Session dbConnection) {
		this.dbConnection = dbConnection;
	}

	public List<WeddingEvent> getWeddingEventsBetween(Date startInterval, Date endInterval) {
		List<WeddingEvent> weddingEvents = new ArrayList<WeddingEvent>();
		try {
			Transaction tx = dbConnection.beginTransaction();
			weddingEvents = dbConnection.createCriteria(WeddingEvent.class)
                                                  .add(Restrictions.ge("dueTime", startInterval))
                                                  .add(Restrictions.lt("dueTime", endInterval))
                                                  .list();
            tx.commit();
	    }
	    catch (Exception e) {
	    	// log error
	    }
	    return weddingEvents;
	}

	public List<DeviceToken> getDeviceTokensForWeddingEvent(WeddingEvent weddingEvent) {
		Event event = weddingEvent.getEvent();
		List<DeviceToken> deviceTokens = new ArrayList<DeviceToken>();
		try {
			Transaction tx = dbConnection.beginTransaction();
	        deviceTokens = dbConnection.createCriteria(DeviceToken.class).add(
	                Restrictions.eq("Wedding", weddingEvent.getWedding())).list();
	        tx.commit();
	    } catch (Exception e) {
	    	// log error
	    }
	    return deviceTokens;
	}
}