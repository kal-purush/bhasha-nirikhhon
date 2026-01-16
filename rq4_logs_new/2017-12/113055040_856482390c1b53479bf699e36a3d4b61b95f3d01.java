package bgu.spl.a2.TestClass;

import bgu.spl.a2.Action;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.*;
import java.util.Map;

/**
 * Created by Stav on 12/21/2017.
 */
public class JsonTest {
    public static void main(String []args){
        Gson gson = new Gson();
//        JsonElement json = null;
//        try {
            //json = gson.fromJson(new FileReader(args[0]), JsonElement.class);
//            String jsonInfo = "{\"name\":\"mkyong\", \"age\":33}";
//            JsonReader jsonReader = gson.newJsonReader(new FileReader(args[0]));
//            JsonReader jsonReader = gson.newJsonReader(new StringReader(jsonInfo));
//            //jsonReader.beginArray();
//            String bla = gson.fromJson(jsonReader, String.class);
//        String jsonInString = "{'name' : 'mkyong' , " +
//                "'action' : 'OpenCourse' }";
//        String json = "{\"name\":\"mkyong\", \"action\":33}";
//        String json2 = "{\n" +
//                "\"Type\":\"A\",\n" +
//                "\"Sig Success\": \"1234666\",\n" +
//                "\"Sig Fail\": \"999283\"\n" +
//                "},\n" +
//                "{\n" +
//                "\"Type\":\"B\",\n" +
//                "\"Sig Success\": \"4424232\",\n" +
//                "\"Sig Fail\": \"5555353\"\n" +
//                "}";
//        Staff staff = gson.fromJson(json, Staff.class);
//        String[] ss = gson.fromJson(json2, String[].class);
    //    jsonReader.beginArray();

        String json = "{\"name\":\"mkyong\", \"age\":33}";
        Map<String, Object> map = gson.fromJson(json, new TypeToken<Map<String, Object>>(){}.getType());
        map.forEach((x,y)-> System.out.println("key : " + x + " , value : " + y));

//            System.out.println(staff.getAction());
            //gson
//            if (jsonReader.nextString() == "Action:Open Course"){

//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String result = gson.toJson(json);
//        try {
//            staff = gson.fromJson(staff, new FileReader(args[0]));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        try {
//            FileInputStream fin = new FileInputStream(args[0]);
//            ObjectInputStream ofin = new ObjectInputStream(fin);
//            JsonObject jsonObject = new JsonObject(new JsonToken(fin));
//            ofin.read();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        int num = gson.fromJson(args[0], int.class);
    }

}