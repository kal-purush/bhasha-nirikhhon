/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Lecturer_Data_Access;

import Connection.DBConnector;
import Lecturer_Domain.Lecturer;
import Student_Domain.Student;
import Subject_Domain.Subject;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedList;
import java.util.Vector;

/**
 *
 * @author Pasindu Tennage
 */
public class Lecturer_Data_Access {

    private DBConnector connector;

    public Lecturer_Data_Access(DBConnector connector) {
        this.connector = connector;
    }

    public void addANewLecturer(int id, String name, String picture, String nic, String address, Date dob) throws ClassNotFoundException, SQLException {
        String sql;
        sql = "INSERT INTO lecturer VALUES ('" + id + "', '" + name + "', '" + picture + "','"
                + nic + "', '" + address + "', '" + dob + "')";
        connector.updateTable(sql);
    }

    public void editSubjectList(int id, LinkedList<Subject> subjects) {
            //will be coded after mapping the database
    }

    public void editLecturerProfile(int id, String name, String picture, String nic, String address, Date dob) throws ClassNotFoundException, SQLException {
        String sql;
        sql = "UPDATE details SET lecturer_id='" + id + "',name='"
                + name + "',picture='" + picture + "',nic='" + nic + "',address='" + address
                + "',date_of_birth='" + dob
                + "' WHERE lecturer_id='" + id + "'";
        connector.updateTable(sql);
    }

    public Lecturer getLecturerProfile(int id) throws ClassNotFoundException, SQLException {
        String sql;
        sql = "SELECT * FROM lecturer WHERE '" + "'lecturer_id LIKE '%" + id + "%'";

        //result set to get selected row of the database
        ResultSet rs = connector.getQuerry(sql);

        //create vector to add each field in the student table
        Vector v = new Vector();
        while (rs.next()) {
            v.add(rs.getInt("lecturer_id"));
            v.add(rs.getString("name"));
            v.add(rs.getString("picture"));
            v.add(rs.getString("nic"));
            v.add(rs.getString("address"));
            v.add(rs.getDate("date_of_birth"));

        }
        //create new student object
        Lecturer lecturer = new Lecturer((int) v.get(0), (String) v.get(1), (String) v.get(2), (String) v.get(3),
                (String) v.get(4), (Date) v.get(5));

        return lecturer;
    }

    public Lecturer getLecturerProfile(String Name) throws SQLException, ClassNotFoundException {
        String sql;
        sql = "SELECT * FROM lecturer WHERE '" + "'name LIKE '%" + Name + "%'";

        //result set to get selected row of the database
        ResultSet rs = connector.getQuerry(sql);

        //create vector to add each field in the student table
        Vector v = new Vector();
        while (rs.next()) {
            v.add(rs.getInt("lecturer_id"));
            v.add(rs.getString("name"));
            v.add(rs.getString("picture"));
            v.add(rs.getString("nic"));
            v.add(rs.getString("address"));
            v.add(rs.getDate("date_of_birth"));

        }
        //create new student object
        Lecturer lecturer = new Lecturer((int) v.get(0), (String) v.get(1), (String) v.get(2), (String) v.get(3),
                (String) v.get(4), (Date) v.get(5));

        return lecturer;
    }

}