public class Employee {
    private String name;
    private String secondName;
    private String fathersName;
    private double salary;
    private int division;
    private final int id;
    static int counter;

    public Employee(String name, String secondName, String fathersName, double salary, int division) {
        this.name = name;
        this.secondName = secondName;
        this.fathersName = fathersName;
        this.salary = salary;
        this.division = division;
        this.id = counter++;

    }

    public String getName() {
        return this.name;
    }

    public String getSecondName() {
        return this.secondName;
    }

    public String getFathersName() {
        return this.fathersName;
    }

    public double getSalary() {
        return this.salary;
    }

    public int getDivision() {
        return this.division;
    }

    public int getId() {
        return this.id;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public void setDivision(int division) {
        this.division = division;
    }

    @Override
    public String toString() {
        return "Name " + this.name + " Surname" + this.secondName + " Fathersname" + this.fathersName + " Division" + this.division + " Salary" + this.salary + " ID" + this.id ;
    }
}