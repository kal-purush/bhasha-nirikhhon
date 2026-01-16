package controller;

import model.Model;

import java.util.ArrayList;

public class Controller {
    public static String login (String email, String password) {
        return Model.login(email, password);
    }

    public static void refresh (int id) {
        Model.refresh(id);
    }

    public static Employee selectWhereEmployee (int id) {
        return Model.selectWhereEmployee(id);
    }

    public static ArrayList<Leaves> selectWhereLeaves (int idEmployee) {
        return Model.selectWhereLeaves(idEmployee);
    }

    public static void deleteLeaves (int id) {
        Model.deleteLeaves(id);
    }

    public static void insertLeaves (Leaves leaves) {
        Model.insertLeaves(leaves);
    }
}