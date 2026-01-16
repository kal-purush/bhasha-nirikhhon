package database;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoDb implements Database {
	private MongoClient mongoClient;
	private MongoDatabase mDatabase;
	private MongoCollection<Document> statisticsCollection;
	private static final Logger LOG = Logger.getLogger(MongoDb.class);

	// infos for mongodb
	public MongoDb() {
		try {
			mongoClient = new MongoClient("localhost", 27017);
			mDatabase = mongoClient.getDatabase("mydb");
			statisticsCollection = mDatabase.getCollection("statistics");
		} catch (MongoException e) {
			LOG.error("Mongo Connection Error", e);

		}
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	@Override
	public void save(String dateStr, String word, int frequency) { // insert
																	// process
																	// date-word-frequency
		try {
			Date date = dateConverter(dateStr);
			statisticsCollection
					.insertOne(new Document().append("date", date).append("word", word).append("frequency", frequency));

		} catch (MongoWriteException e) {
			LOG.error("Data couldn't be inserted.", e);

		}
	}

	@Override
	public void delete() {// Delete All documents from collection Using blank
							// BasicDBObject
		try {
			statisticsCollection.deleteMany(new Document());
			LOG.info("All data deleted");
		} catch (MongoWriteException e) {
			LOG.error("Data couldn't be deleted.", e);
		}

	}

	@Override
	public void update(String dateStr, String word, int frequency) { // update
																		// and
																		// increment
																		// the
																		// frequency
		Date dateConverted = dateConverter(dateStr);
		try {
			statisticsCollection.updateOne(new Document("date", dateConverted).append("word", word),
					new Document("$inc", new Document("frequency", frequency)));
		} catch (MongoWriteException e) {
			LOG.error("Data couldn't be updated.", e);
		}
	}

	@Override
	public void fetch(String date) { // frequency sorted from a date for all
										// words
		Date dateConverted = dateConverter(date);

		FindIterable<Document> iterable = statisticsCollection.find(new Document().append("date", dateConverted))
				.sort(new Document().append("frequency", -1));// filter in
																// document
																// example new
																// Document("borough",
																// "Manhattan")
		printerOfFindIterable(iterable);

	}

	public void fetch(String date, int limit) { // frquency sorted for top 10
												// word
		Date dateConverted = dateConverter(date);

		FindIterable<Document> iterable = statisticsCollection.find(new Document().append("date", dateConverted))
				.sort(new Document().append("frequency", -1)).limit(limit);// filter
																			// in
																			// document
																			// example
																			// new
																			// Document("borough",
																			// "Manhattan")

		printerOfFindIterable(iterable);
	}

	public void fetch() { // frequency sorted for all the world

		FindIterable<Document> iterable = statisticsCollection.find(new Document())
				.sort(new Document().append("frequency", -1));
		printerOfFindIterable(iterable);
	}

	private void printerOfFindIterable(FindIterable<Document> iterable) { // which
																			// provide
																			// to
																			// print
																			// document
																			// in
																			// collection
		// TODO if nothing to show say in here There is nothing to show
		System.out.println("----Date----" + "\t\t\t\t" + "----Word----" + "\t" + "----Frequency----");

		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(
						document.get("date") + "\t\t" + document.get("word") + "\t\t" + document.get("frequency"));
			}
		});
	}

	public long fetchCount(String dateStr, String word) { // give a count from a
															// date which
															// contain "word"
		Date date = dateConverter(dateStr);

		Bson filter = new Document().append("date", date).append("word", word);

		return statisticsCollection.count(filter);// sayısını

	}

	private Date dateConverter(String dateStr) {
		Boolean flag = false;

		BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
		SimpleDateFormat format1 = new SimpleDateFormat("dd-MMM-yy");
		Date date = null;
		do {
			if (flag) {
				System.out.println("BE CAREFUL.Insert a date of day like :  17 Mar 2016\n");
				try {
					dateStr = input.readLine();
				} catch (IOException e) {
					LOG.error("Input(String) couldn't convert to date. ",e);
				}
			}
			try {
				date = format1.parse(dateStr.replaceAll("\\s+", "-"));
				flag = false;
				LOG.debug("Input(String) converted date successfully.");

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				flag = true;
				LOG.error("Input(String) couldn't convert to date.It will be requested again. ",e);
			} // date is the our object's date
		} while (flag);
		return date;
	}
}