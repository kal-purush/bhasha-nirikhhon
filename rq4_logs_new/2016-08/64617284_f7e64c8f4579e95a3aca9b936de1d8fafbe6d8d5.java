package epam.ja.krk.mongo;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.jfairy.Fairy;


import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by egolesor on 27.07.16.
 */
public class JasDBTest {


    public static int TESTING_LIMITATION;

    final List<DBCollection> collectionList = new LinkedList<>();
    final List<DBObject> objectList = new LinkedList<>();

    //testing 1000 document writing on the mongo database
    public static void main(String ... args ){

        int argsSize=args.length-1;
        JasDBTest jasDBTest = new JasDBTest();

        TESTING_LIMITATION = Integer.valueOf(args[0]);
        String[] collectionNameList = new String[argsSize];
        String[] jsonFileList = new String[argsSize];
        String[] temp;
        for(int counter=0; counter<argsSize; counter++){
            temp=args[counter+1].split("=");
            collectionNameList[counter] = temp[0];
            jsonFileList[counter] = temp[1];
        }

        jasDBTest.mongo(collectionNameList);

        // put the data in the loop

        jasDBTest.prepareObjects(jsonFileList);

        System.out.println("Putting Data");
        jasDBTest.putData();

        System.out.println("Getting Data");
        jasDBTest.getData();
    }


    public void mongo(String[] collectionNameList){

        // connecting to the database
        MongoClient mongoClient = null;
        try {
            mongoClient = new MongoClient("localhost" , 27017);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        // getting the database
        DB db = mongoClient.getDB("test");
        boolean auth = db.authenticate("username", "password".toCharArray());

        Random random = new Random();
        for (String name : collectionNameList){
            //db.getCollection(name).drop();
            String collectionName = name+random.nextLong();
            System.out.printf("Collection : " + collectionName + " [%s] ", collectionList.add(db.createCollection(collectionName, new BasicDBObject())));
            System.out.println();
        }

    }

    // putting datas
    public void putData(){
        long start = System.currentTimeMillis();
        int counter = 0;
        for(int loopCounter=0; loopCounter<TESTING_LIMITATION; loopCounter++){
            if(counter>=collectionList.size()){
                counter=0;
            }
            BasicDBObject object = new BasicDBObject();
            object.putAll(objectList.get(counter));
             collectionList.get(counter).insert(object);
        }

        long spent = System.currentTimeMillis() - start;
        System.out.println(spent + " millisecond to put "+ TESTING_LIMITATION + " data in " + collectionList.size() + " collections");
    }

    public void prepareObjects(String[] fileName){
        int counter = 0;
        char[] text ;
        StringBuilder builder = new StringBuilder();
        for(DBCollection collection : collectionList){
            try {
                File file = new File("src/" + fileName[counter]);
                text = new char[(int) file.length()];
                builder.delete(0, builder.length());
                FileReader reader = new FileReader(file);
                reader.read(text);
                for(char character: text){
                    builder.append(character);
                }
                DBObject dbObject = (DBObject) JSON.parse(builder.toString());
                objectList.add(dbObject);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            counter++;
        }
    }

    // getting data
    public void getData(){
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        long start = System.currentTimeMillis();//mxBean.getCurrentThreadCpuTime();
        int counter = 0;
        for(int loopCounter=0; loopCounter<TESTING_LIMITATION; loopCounter++){
            if(counter>=collectionList.size()){
                counter=0;
            }
            collectionList.get(0).findOne();
        }

        long spent = System.currentTimeMillis() - start;
        System.out.println(spent + " millisecond to get " + TESTING_LIMITATION + " data from " + collectionList.size() + " collections");

        System.out.println("deleting the created collections");
        collectionList.forEach(DBCollection::drop);


    }

}