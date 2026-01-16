package org.example.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.example.Student;
import org.example.University;

import java.util.List;

public class JsonUtil {

    private JsonUtil() {

    }

    public static String studentToJsonSerializer(Student student) {

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(student);
    }

    public static String universityToJsonSerializer(University university) {

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(university);
    }

    public static String studentsToJsonSerializer(List<Student> students) {

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(students);
    }

    public static String universitiesToJsonSerializer(List<University> universities) {

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(universities);
    }

    public static Student studentFromJsonDeserializer(String json) {

        Gson gson = new Gson();
        return gson.fromJson(json, Student.class);
    }

    public static University universityFromJsonDeserializer(String json) {

        Gson gson = new Gson();
        return gson.fromJson(json, University.class);
    }

    public static List<Student> studentsFromJsonDeserializer(String json) {

        Gson gson = new Gson();
        return gson.fromJson(json, new TypeToken<List<Student>>(){}.getType());
    }

    public static List<University> universitiesFromJsonDeserializer(String json) {

        Gson gson = new Gson();
        return gson.fromJson(json, new TypeToken<List<University>>(){}.getType());
    }
}