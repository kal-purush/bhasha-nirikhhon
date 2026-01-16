public class Person {
    private String SSN,name;
    private char gender;
    private Person Spouse;

    public Person(String SSN, String name, char gender) {
        this.SSN = SSN;
        this.name = name;
        this.gender = gender;
        this.Spouse = null;
    }

    public Person() {
    }

    public void marry(Person person){
        if (person.Spouse == null && this.Spouse == null && this != person && this.gender != person.gender){
            person.Spouse = this;
            this.Spouse = person;
        }
    }

    public void divorce(){
        this.Spouse.Spouse = null;
        this.Spouse=null;
    }

    public String display() {
        return "Person{" +
                "SSN='" + SSN + '\'' +
                ", name='" + name + '\'' +
                ", gender=" + gender +
                '}';
    }

    public void displaySpouse(){
        if (this.Spouse != null){
            System.out.println(this.Spouse.display());
        }else {
            System.out.println("There is no spouse");
        }
    }

    public static void main(String[] args) {
        Person Alice = new Person("79","Alice",'F');
        Person Bob = new Person("39","Bob",'M');
        Person Carol = new Person("55","Carol",'F');


        Alice.marry(Bob);
        Alice.divorce();
        Bob.marry(Carol);
        Carol.displaySpouse();

    }
}