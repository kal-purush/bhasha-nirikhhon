package homework_nr_16;

public class Person {
    private String name;
    private int age;

    public Person(String name, int age, double rating) {
        this.name = name;
        this.age = age;
        this.rating = rating;
    }

    private double rating;



    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }
}