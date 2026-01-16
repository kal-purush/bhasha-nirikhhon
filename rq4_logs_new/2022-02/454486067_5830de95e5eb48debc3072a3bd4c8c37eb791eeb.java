public class Employee {
    //public static  void main (String ars[]){

    String IFO;
    String Profession;
    String email;
    int phone;
    int salary;
    int age;

    // Task02
    public Employee(String IFO, String profession, String email, int phone, int salary, int age) {
        this.IFO = IFO;
        this.Profession = profession;
        this.email = email;
        this.phone = phone;
        this.salary = salary;
        this.age = age;
    }


    public void person() {
        System.out.println(IFO + "Name:" + Profession + "Profession:" + email + "Email:"+ phone + "NumberPhone:"+
                salary + " " + age + "Age:");

    }


}