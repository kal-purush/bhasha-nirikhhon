package LAB_PBO_2024.Praktikum.Praktikum_02;

public class Nomor1 {
    //atribute
    public static class Person {
        private String name;
        private int age;
        private boolean isMale;

    //Method untuk mendapatkan name
    public String getName() {
        return name;
    }

    //Method unntuk mendapatkan umur
    public int getAge() {
        return age;
    }

    //Method untuk mendapatkan gender
    public String getGender() {
        return isMale ? "Male" : "Female";
    }

    //Main System
    public static void main(String[] args) {
        //contoh
        Person person = new Person();

        //Mengatur nilai atribute dengan setter
        person.name = "Nafil";
        person.age = 19;
        person.isMale = true;

        //Menampilkan hasil dengan getter
        System.out.println("Name: " + person.getName());
        System.out.println("Age: " + person.getAge());
        System.out.println("Gender: " + person.getGender());

    }
    }
}