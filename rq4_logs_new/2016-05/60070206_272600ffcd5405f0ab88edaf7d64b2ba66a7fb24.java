package com.galaksiya.newsObserver.master;


import com.galaksiya.newsObserver.database.MongoDb;

public class DbHelper {
	private MongoDb mongoDbHelper;
	private NewsChecker newsCheckerTest = new NewsChecker();
	public DbHelper(){
		mongoDbHelper = new MongoDb();
	}
	public DbHelper(String collection){
		mongoDbHelper = new MongoDb(collection);
	}
	public boolean addDatabase(String datePerNew,String word,int frequency){//Mesela bu mainProcesstede var bu yüzden database helpder diyip böyle bir class açmamız faydalı olur
		boolean flagSuccessful;
		if(!newsCheckerTest.canConvert(datePerNew)) return false;
		boolean alreadyInsertedToDatabase=mongoDbHelper.contain(datePerNew, word) >= 1;
		if(alreadyInsertedToDatabase){  //check key value per new to increment or add  
			flagSuccessful=mongoDbHelper.update(datePerNew, word, frequency);//if it has alreadt inserted just increment  
		}
		else {
			flagSuccessful=mongoDbHelper.save(datePerNew, word, frequency);//if not insert it
		}
		return flagSuccessful;
	}
}