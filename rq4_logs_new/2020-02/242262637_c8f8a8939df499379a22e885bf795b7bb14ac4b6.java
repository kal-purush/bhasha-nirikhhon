package com.seblong.okr.entities;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Data;

@Document(collection = "c_okr_history")
@CompoundIndex(name = "idx_date_user_period", def = "{ 'user' : 1, 'period' : 1, 'date' : -1 }")
@Data
public class OKRHistory {

	@Id
	private ObjectId id;
	
	//yyyyMMdd
	private String date;
	
	private String user;
	
	private String period;
	
	private OKR okr;
	
	private long created;

	@PersistenceConstructor
	public OKRHistory(String date, String user, String period, OKR okr, long created) {
		this.date = date;
		this.user = user;
		this.period = period;
		this.okr = okr;
		this.created = created;
	}

	public OKRHistory(String date, OKR okr) {
		this.date = date;
		this.okr = okr;
		this.user = okr.getUser();
		this.period = okr.getPeriod();
		this.created = System.currentTimeMillis();
	}
	
	public void update(OKR okr) {
		this.okr = okr;
		this.created = System.currentTimeMillis();
	}
	
}