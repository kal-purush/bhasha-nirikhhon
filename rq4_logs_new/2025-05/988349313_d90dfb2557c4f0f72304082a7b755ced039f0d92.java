abstract class HospitalRole {
    abstract void performeDuties();
}

interface ShiftTime {
    String getShiftingTime();
}

class Staff extends HospitalRole {
    private int id;
    private String name;
    private int salary;

    public Staff(int id, String name, int salary) {
        this.id =id;
        this.name = name;
        this.salary = salary;
    }

    

    public void performeDuties() {
        System.out.println("staff performe their duty");
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public int getSalary() {
        return salary;
    }
    
    public String calculateSalary() {
        return "this is calculated slary";
    }
}

class Doctor extends Staff implements ShiftTime{

    public Doctor(int id, String name, int salary) {
        super(id,name,salary);
    }

    public String calculateSalary() {
       return  "Doctor salary: "+getSalary();
    }

    public void performeDuties() {
        System.out.println("Doctor treat the patainets");
    }

    public String getShiftingTime() {
        return "9AM-5PM";
    }
}
class Nurse extends Staff implements ShiftTime{
    public Nurse(int id, String name, int salary) {
        super(id,name,salary);
    }

    public String calculateSalary() {
        return "Nurse Salary"+getSalary();
    }

    public void performeDuties() {
        System.out.println("Nurse help the doctor");
    }

    public String getShiftingTime() {
        return "8AM-6PM";
    }
}

class AdminStaff extends Staff implements ShiftTime{
    public AdminStaff(int id, String name, int salary) {
        super(id,name,salary);
    }

    public void performeDuties() {
        System.out.println("Admin manges the pateint report");    }

    public String calculateSalary() {
        return "Admin Salary: "+getSalary();
    }
    public String getShiftingTime() {
        return "9AM-5PM";
    }
}

class HospitalManagement {
    public static void main(String[] args) {
        Staff[] staff = new Staff[] {
            new Doctor(123,"ramesh",50000),
            new Nurse(234,"sunita",20000),
            new AdminStaff(345,"amit",30000)
        };

        for(int i=0;i<staff.length;i++) {
            System.out.println(staff[i].calculateSalary());
                 ShiftTime s = (ShiftTime)staff[i];
            System.out.println(s.getShiftingTime());

        }
    }
}