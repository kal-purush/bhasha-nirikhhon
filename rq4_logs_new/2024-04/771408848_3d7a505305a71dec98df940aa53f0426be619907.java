class Person {
    private String nama;
    private int age;
    private boolean isMale;


    public void setNama(String nama) {
        this.nama = nama;
    }
    public String getNama() {
       return nama;
    }
    public void setAge(int age) {
        this.age = age;
    }
    public int getAge() {
        return age;
    }
    public void setGender(boolean isMale) {
        this.isMale = isMale;
    }
    public String getGender(){
        return isMale ? "Laki-laki" : "Perempuan";
    }
    


    }

   
public class TP1_H071230137 {
    public static void main(String[] args) {
    Person person1 = new Person();
    person1.setNama("Alief");
    person1.setAge(18);
    person1.setGender(true);
    
    System.out.println("==================================================");
    System.out.println("Nama anda adalah = " + person1.getNama());
    System.out.println("Umur anda adalah = " + person1.getAge());
    System.out.println("Gender anda adalah = " + person1.getGender());
    System.out.println("==================================================");
    
    }


}