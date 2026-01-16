package ja.krk.mongo;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.apache.log4j.Logger;
import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import org.apache.commons.lang.time.StopWatch;

/**
 * Class responsible to test the mongo database.
 */
public class JasDBTest {

    private static Logger logger = Logger.getLogger(JasDBTest.class);

    private String HOST = "10.253.133.126";
    private int PORT = 27017;

    private int testingLimitation;
    final List<DBCollection> collectionList = new LinkedList<>();
    final List<DBObject> objectList = new LinkedList<>();
    private FileWriter writer;
    private DB database;

    public JasDBTest(String HOST, int PORT, int testingLimitation) {
        this(testingLimitation);
        this.HOST = HOST;
        this.PORT = PORT;
    }

    public JasDBTest(int testingLimitation) {
        this.testingLimitation = testingLimitation;
    }

    public void methodTest(String ... args){
        int argsSize=args.length;

        String[] collectionNameList = new String[argsSize];
        String[] jsonFileList = new String[argsSize];
        String[] temp;
        for(int counter=0; counter<argsSize; counter++){
            temp=args[counter].split("=");
            collectionNameList[counter] = temp[0];
            jsonFileList[counter] = temp[1];
        }

        //file to write results
        prepareResultFile();

        logger.debug("Connecting to Mongo database: " + HOST+":"+PORT);
        connectToTheDb(HOST, PORT);

        logger.debug("Creating the collections to test");
        prepareCollections(database, collectionNameList);
        // put the data in the loop
        prepareDatabaseObjectList(jsonFileList);

        insertingDataIntoMongoDB();

        logger.debug("Getting Data");
        getData();

        try {
            writer.write("\n");
            writer.close();
        } catch (IOException e) {
            logger.error(e);
        }
    }

    private void prepareResultFile() {
        String filePath = "src/results";
        logger.debug("Openning file " + filePath);
        File file = new File(filePath);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            writer = new FileWriter(file, true);

        }catch(IOException e){
            logger.error(e);
        }
    }


    public void connectToTheDb(String host, int port){

        // connecting to the database
        MongoClient mongoClient = null;
        try {
            //mongoClient = new MongoClient("10.253.133.126" , 27017);
            mongoClient = new MongoClient(host , port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        // getting the database
        database = mongoClient.getDB("mongo");
    }

    private void prepareCollections(DB db, String[] collectionNameList) {
        Random random = new Random();
        for (String name : collectionNameList){
            String collectionName = name+random.nextLong();
            logger.debug(String.format("Collection : " + collectionName + " [%s] ", collectionList.add(db.createCollection(collectionName, new BasicDBObject()))));
        }
    }

    public void insertingDataIntoMongoDB(){
        logger.debug("START");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        stopWatch.suspend();

        int counter = 0;
        for(int loopCounter = 0; loopCounter< testingLimitation; loopCounter++){
            if(counter>=collectionList.size()){
                counter=0;
            }
            BasicDBObject object = new BasicDBObject();

            object.putAll(objectList.get(counter));
            stopWatch.resume();
            collectionList.get(counter).insert(object);
            stopWatch.suspend();
        }
        logger.debug(String.format("%s to put "+ testingLimitation + " data in " + collectionList.size() + " collections", stopWatch.toString()));
        writeResult("set", stopWatch.toString());
        stopWatch.stop();
        logger.debug("END");
    }

    public void prepareDatabaseObjectList(String[] fileNameList){
        logger.debug("START");
        int counter = 0;
        char[] text ;
        StringBuilder builder = new StringBuilder();
        String filePath;
        for(DBCollection collection : collectionList){
            try {
                filePath = "src/" + fileNameList[counter];
                logger.debug("Openning file " + filePath);
                File file = new File(filePath);
                text = new char[(int) file.length()];
                // empty the string
                builder.delete(0, builder.length());
                FileReader reader = new FileReader(file);
                reader.read(text);
                // write all characters from the file to the string
                for(char character: text){
                    builder.append(character);
                }
                DBObject dbObject = (DBObject) JSON.parse(builder.toString());
                objectList.add(dbObject);
            } catch ( IOException e) {
                logger.debug(e);
            }
            counter++;
        }
        logger.debug("END");
    }

    public void getData(){
        logger.debug("START");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        stopWatch.suspend();
        int counter = 0;
        for(int loopCounter = 0; loopCounter< testingLimitation; loopCounter++){
            if(counter>=collectionList.size()){
                counter=0;
            }
            stopWatch.resume();
            collectionList.get(0).findOne();
            stopWatch.suspend();
        }

        logger.debug(String.format("%s to get " + testingLimitation + " data from " + collectionList.size() + " collections", stopWatch.toString()) );
        writeResult("get", stopWatch.toString());
        logger.debug("deleting the created collections");
        collectionList.forEach(DBCollection::drop);
        logger.debug("END");
    }

    private void writeResult(String operation, String howLong ){
        try {
            writer.write(operation);
            writer.write("\t");
            writer.write(testingLimitation+"");
            writer.write("\t");
            writer.write(collectionList.size()+"");
            writer.write("\t");
            writer.write(howLong);
            writer.write("\n");

        } catch (IOException e) {
            logger.debug(e);
        }

    }

}