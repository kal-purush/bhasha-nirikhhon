package plants;
// Program by Bob L.
// Create a MongoDB database client and add some Plants information to it.
// Then do database operations as mentioned in the main method.

// Jar file to use is mongo-java-driver-3.5.0..jar
// More details at:
// https://mongodb.github.io/mongo-java-driver/

// To run:
// Have a custom directory or make one and call it some name, eg "JavaFiles1"
// Make a "plants" folder in it.
// Have this Java file in that "plants" folder,
// add the jar file in the root. i.e. in "JavaFiles1".
// Be in the root directory, i.e. "JavaFiles1"
// and using Cygwin, compile, then run. i.e.:
// $ javac -cp "mongo-java-driver-3.5.0.jar" plants/DisplayPlantsDetails.java
// java -cp "mongo-java-driver-3.5.0.jar" plants/DisplayPlantsDetails.java

// Result:
// afte running, you will see the results
// of all methods that were called in main method.


import java.util.Scanner;
import java.util.Date;
import java.util.LinkedList;
 
import com.mongodb.BasicDBObject; //4
import com.mongodb.DB; //2
import com.mongodb.DBCollection; //3
import com.mongodb.DBCursor; //5
import com.mongodb.MongoClient; //1
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import com.mongodb.DBObject;

import com.mongodb.AggregationOutput; //used upto version 3.5 of the driver.

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;



public class DisplayPlantsDetails {

   @SuppressWarnings("deprecation")
   public static void main(String[] args) {
      System.out.println ("\n Start \n");
      //enterCharacteristics();
      try {
         // Connecting To The MongoDb Server Listening On A Default Port (i.e. 27017).
         MongoClient mongoClntObj = new MongoClient("localhost", 27017);
         
         // Get MongoDb Database. If The Database Doesn't Exists, MongoDb Will Automatically Create It For You
         DB dbObj = mongoClntObj.getDB("plants_version1");
   
         // Get MongoDb Collection. If The Collection Doesn't Exists, MongoDb Will Automatically Create It For You
         DBCollection collectionObj = dbObj.getCollection("plants");
         
         // //With every run, drop the collection and reset.
         // Drop collection from the MongoDB database.
         if (dbObj.collectionExists("plants")) {
            collectionObj.drop();
            collectionObj = dbObj.getCollection("plants");
            System.out.println("\n dropped table and created it again.");
         }

         //insert several documents into collection, pass on collectionObj:
         doInsertOperations(collectionObj);
         
         //write out documents to terminal
         writeOutDocuments(collectionObj);
         
         //return name of all plants having category "leaves"
         findStringInArrayAndPrintWhatField(collectionObj,
                                  "other_names",
                                  "gol_sonbol",
                                  "name");
          //give flower "Hyacinth" an increase in stars by 1 and print                        
         updateADoc_GivenFieldAndFieldValue_AndIncreaseAFieldByValueAndPrintAll (
                                 collectionObj,
                                 "name",
                                 "Hyacinth",
                                 "stars",
                                 1);
                                 
         //return all flowers having 2 or more stars. Print only name and star
         findDocs_use_gt_operator(collectionObj,
                                 "stars",
                                 2);

         System.out.println ("\n done.");
         
      } catch (MongoException mongoExObj) {
         mongoExObj.printStackTrace();
      }

   }
   
   
   /**
   * characteristics to mention for each plant.
   * eg. plant leaves color, plant leaves size, flower shape, time purchased
   */
   static void enterCharacteristics() {
      System.out.println ("Please enter the name of " +
                           "characteristics or properties you like to have " +
                           "or mention for each plant...");
      Scanner sc = new Scanner(System.in);
      ArrayList<String> props = new ArrayList();
      String prop = new String();
      while ( !prop.equals("end") )  {
         prop = sc.next();
         props.add(prop);
         System.out.println ("added " + prop + " to properties.\n" +
            "Type another property or type end to end adding properties.");
      }
   }
   
   
   /**
   * Insert several documents into the MongoDB database
   * @param collectionObj  the collection that we are adding documents to.
   */
   static void doInsertOperations(DBCollection collectionObj) {         
      String document1 = "{" +
         		" \"name\" : \"Anthurium\", " +
         		" \"leaves_color\" : \"green\", " +
         		" \"other_names\" : [ \"gol_Sheypoori\" ], " +
               " \"stars\": 1 }";
        
      String document2 = "{\"name\" : \"Orchide\", " +
         		"\"leaves_colors\" : \"green\", " +
         		"\"other_names\" : [	\"gol_orkide\" ], " +
               " \"stars\": 1 }";

      String document3 = "{\"name\" : \"Hyacinth\", " +
         		"\"leaves_colors\" : \"green\", " +
         		"\"other_names\" : [ \"gol_sonbol\" ], " +
               " \"stars\":1 }";
      
      DBObject doc1 = (DBObject) JSON.parse(document1);
      collectionObj.insert(doc1);

      DBObject doc2 = (DBObject) JSON.parse(document2);
      collectionObj.insert(doc2);

      DBObject doc3 = (DBObject) JSON.parse(document3);
      collectionObj.insert(doc3);
                                
      System.out.println("\n done inserting 3 documents.");
               
   }

  
   /** 
   *  Print out all documents in the collection
   *  @param: collectionObj   the database collection object
   */
   static void writeOutDocuments(DBCollection collectionObj) {
         System.out.println("\n print all documents...");       
         DBCursor cursorObj = collectionObj.find();
         try {
            while(cursorObj.hasNext()) {
               System.out.println(cursorObj.next());
             }
         } finally {
            cursorObj.close();
         }
   }
  

   /** Given an array name in the collection, as well as a term to search for,
   *     return all the resulting documents and print out only the name of the
   *     field mentioned, i.e. printWhatField. This filed is used in the
   *     projection part of the results that DBCursor will point to.
   *  @param collectionObj
   *  @param arrayName        the array name for the query to search in.
   *  @param term             the term that should be searched.
   *  @param printWhatField   the field used in the projection part of query.
   */  
   static void findStringInArrayAndPrintWhatField( DBCollection collectionObj,
                                    String arrayName, String term, String printWhatField) {
        System.out.println("\n find: \"" + term + "\" in array: \"" + arrayName + "\"" +
            " and print the document's \"" + printWhatField + "\" field...");                           
        
        //set array name and fields to query
        BasicDBObject inQuery = new BasicDBObject();
        List<String> list = new ArrayList<String>();
        list.add(term);
        inQuery.put(arrayName, new BasicDBObject("$in", list));
        
        //set the projection to use in finding
        BasicDBObject projection = new BasicDBObject();
        projection.put("_id",0);
        projection.put(printWhatField, 1);
        
        //point to result using DBCursor
        DBCursor cursor = collectionObj.find(inQuery, projection);
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }                            
   }
   
   
   /** Update a document using the $inc keyword:
   *  Get a field and value in order to find the right document.
   *  Then update a field given its name and the value to increase by.
   *  @param   collectionObj     The database collection object
   *  @param   findWhichField    Field to search for with a given value.
   *  @param   withWhichValue    Field value to search with the given field.
   *  @param   whichFieldToUpdate Name of field to increase with a given int.
   *  @param   whatIncreaseValue  The value to increase by. 
   */
  static void updateADoc_GivenFieldAndFieldValue_AndIncreaseAFieldByValueAndPrintAll (
                                 DBCollection collectionObj,
                                 String findWhichField,
                                 String withWhichValue,
                                 String whichFieldToUpdate,
                                 int whatIncreaseValue) {
      System.out.println("\n update the document(s), " +
         "given a field name:\n " +
         "\"" + findWhichField + "\"" + " with value: \"" + withWhichValue +
         "\",\n and update its field: \"" + whichFieldToUpdate + "\" with " + 
         String.valueOf(whatIncreaseValue) + "...");
          
      BasicDBObject whichFieldToSearchObj = new BasicDBObject();
      whichFieldToSearchObj.append (findWhichField, withWhichValue);

      
      BasicDBObject toUpdateObj =
         new BasicDBObject().append("$inc",
         new BasicDBObject().append(whichFieldToUpdate, whatIncreaseValue) );
      collectionObj.update( whichFieldToSearchObj, toUpdateObj);
      
      writeOutDocuments(collectionObj);                                        
   }                            
   
   
   /**
   *  Aggregate documents using the $match and $project operators.
   *  hard coded: not returning the "_id", returning "name" and "stars".
   *  @param collectionObj  The database collection object
   *  @param whichFieldStr  The field that the $gte operator should check.
   *  @param greaterThanOrEqualToInt   $gte operator applies to this integer.
   */
   static void findDocs_use_gt_operator(DBCollection collectionObj,
                                         String whichFieldStr,
                                         int greaterThanOrEqualToInt) {
      System.out.println("\ngroup the documents with stars $gte 2. Show name and stars...");
      
      //prepare object with $match:
      BasicDBObject matchObj = new BasicDBObject();
      matchObj.append(  "$match", new BasicDBObject (whichFieldStr,
                           new BasicDBObject ( "$gte", greaterThanOrEqualToInt )
                        )
                     );
      
      //prepare object with $project:
      BasicDBObject projectDetailsObj = new BasicDBObject();
      projectDetailsObj.append ("_id", 0);
      projectDetailsObj.append ("name", 1);
      projectDetailsObj.append ("stars", 1);
      
      BasicDBObject projectObj = new BasicDBObject();
      projectObj.append( "$project", projectDetailsObj ); 
                        
      //AggregationOutput output = collectionObj.aggregate ( Arrays.asList(matchObj) );
      AggregationOutput output = collectionObj.aggregate ( 
                                    Arrays.asList(matchObj, projectObj)
                                 );
      
      //Print out all objects:      
      for (DBObject result : output.results() ) {
         System.out.println (result);
      }
   }

}