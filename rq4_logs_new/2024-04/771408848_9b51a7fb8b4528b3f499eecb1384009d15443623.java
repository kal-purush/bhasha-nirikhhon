class Person {

    private String name;
    private Boolean ismale;
    private Integer age;

    public void SetName(String newName) {
        if (newName != null && !newName.isEmpty()) {
            this.name = newName;
        } else {
            System.out.println("Error: Name Tidak bisa kosong.");
        }
    }

    public String GetName() {
        return this.name;
    }

    public void SetAge(Integer newAge) {
        if (newAge != null && newAge >= 0) {
            this.age = newAge;
        } else {
            System.out.println("Error: Tidak bisa kosong atau negatif.");
        }
    }

    public Integer GetAge() {
        return this.age;
    }

    public void SetGender(String newGender) {
        if (newGender != null) {
            if (newGender.toLowerCase().equals("male")) {
                this.ismale = true;
            } else if (newGender.toLowerCase().equals("female")) {
                this.ismale = false;
            } else {
                System.out.println("Error: Gender Invalid.");
            }
        } else {
            System.out.println("Error: tidak bisa kosong.");
        }
    }

    public String GetGender() {
        return this.ismale ? "Male" : "Female";
    }
}

public class Soal1 {
    public static void main(String[] args) {
        Person person = new Person();
        try {
            person.SetName("John");
            person.SetAge(25);
            person.SetGender("male");
            
            System.out.println("Name: " + person.GetName());
            System.out.println("Age: " + person.GetAge());
            System.out.println("Gender: " + person.GetGender());
            
            person.SetName(null);
            person.SetAge(-10);
            person.SetGender(null);

        } catch (Exception e) {
            System.out.println(e);
            return;
        }

    }
}