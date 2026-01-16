package org.example.bot;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.checkerframework.checker.units.qual.A;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class JsonHandler {


    static String filePath = "src/main/resources/registro_recordatorio.json";

    public static void writeId(long id) throws IOException {

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(Files.newOutputStream(Path.of(filePath))));

        writer.beginObject();
        writer.setIndent("  ");

        writer.name("registered_id");
        writer.beginArray();

        if (!readJson().isEmpty()) {
            for (long currId : readJson()) {

                writer.value(currId);
            }
        }

        writer.value(id);

        writer.endArray();
        writer.endObject();
        writer.close();
    }


    public static ArrayList<Long> readJson() {
        ArrayList<Long> idList = new ArrayList<>();

        try {
            Gson gson = new GsonBuilder().create();
            JsonReader reader = gson.newJsonReader(new InputStreamReader(Files.newInputStream(Path.of(filePath))));

            reader.setLenient(true);

            reader.beginObject();
            reader.nextName();
            reader.beginArray();


            while (reader.hasNext()) {
                idList.add(reader.nextLong());
            }

            reader.endArray();
            reader.endObject();
            reader.close();

            System.out.println(Arrays.toString(idList.toArray()));


        } catch (IOException e) {
            e.printStackTrace();
        }
        return idList;
    }


    public static void removeId(long id) throws IOException {

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(Files.newOutputStream(Path.of(filePath))));

        writer.beginObject();
        writer.setIndent("  ");

        writer.name("registered_id");
        writer.beginArray();

        if (!readJson().isEmpty()) {

            readJson().stream().filter(e -> e != id).forEach(e -> {
                try {
                    writer.value(e);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            });
        }

        writer.endArray();
        writer.endObject();
        writer.close();
    }
}