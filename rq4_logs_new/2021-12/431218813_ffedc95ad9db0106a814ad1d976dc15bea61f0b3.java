package person;

public class PersonModel {
   private String name;
   private boolean isWorking;
   private String job;
   private char grade;
   private double age;

    public PersonModel(String name, boolean isWorking, String job, char grade, double age) {
        this.name = name;
        this.isWorking = isWorking;
        this.job = job;
        this.grade = grade;
        this.age = age;
    }


    public PersonModel() {
    }

    public String getName() {
        return name;
    }

   public void setName(String name) {
        this.name = name;
    }

    public boolean isWorking() {
        return isWorking;
    }

    public void setWorking(boolean working) {
        isWorking = working;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public double getAge() {
        return age;
    }

    public void setAge(double age) {
        this.age = age;
    }

    public char getGrade() {
        return grade;
    }

    public void setGrade(char grade) {
        this.grade = grade;
    }

    @Override
    public String toString() {
        return "PersonModel{" +
                "name='" + name + '\'' +
                ", isWorking=" + isWorking +
                ", job='" + job + '\'' +
                ", grade=" + grade +
                ", age=" + age +
                '}';
    }
}