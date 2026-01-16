package Entities;

/**
 * Created by monicaramirez on 5/10/15.
 */
public class Students {
    private int id;
    private String name;
    private String career;
    private int numberCreditsApprove;
    private double average;

    public Students() {

    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCareer() {
        return career;
    }

    public void setCareer(String career) {
        this.career = career;
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
}