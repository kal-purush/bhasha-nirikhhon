package controllers;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by monicaramirez on 5/10/15.
 */
public class Students {
    private String name;
    private String carer;
    private int numberCreditsApprove;
    private double average;
    List <Students> listStudents = new ArrayList<Students>();

    Students() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCarer() {
        return carer;
    }

    public void setCarer(String carer) {
        this.carer = carer;
    }

    public int getNumberCreditsApprove() {
        return numberCreditsApprove;
    }

    public void setNumberCreditsApprove(int numberCreditsApprove) {
        this.numberCreditsApprove = numberCreditsApprove;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public  boolean saveStudent(Students students) {
        boolean status = true;
        try {
            listStudents.add(students);
        }catch (Exception e){
            status = false;
        }
        return status;
    }

    public List<Students> showStudents() {
        return listStudents;
    }
}