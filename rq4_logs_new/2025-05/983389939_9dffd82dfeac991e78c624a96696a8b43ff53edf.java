package Employee;

public class employee {
    String name;
    int id;
    double salary;

    public employee(String name, int id, double salary) {
        this.name = name;
        this.id = id;
        this.salary = salary;
    }

    public void displayInfo() {
        System.out.println("Employee ID: " + id);
        System.out.println("Employee Name: " + name);
        System.out.println("Salary: " + salary);
    }

    public static void main(String[] args) {
        employee e = new employee("abc", 123, 5000);
        e.displayInfo();
    }
}