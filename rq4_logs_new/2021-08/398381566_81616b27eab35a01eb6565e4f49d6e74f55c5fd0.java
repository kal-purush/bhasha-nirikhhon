package formocks;

public class House {

    private Person person;

    public House(Person person) {
        this.person = person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }

    public House() {
    }

    public String getNameOfP(){
        return person.getName();
    }

    public int getAgeOfP(){
        return person.getAge();
    }

    public String getResult() {
        return "" + getAgeOfP() + getNameOfP();
    }

    public static String useStatic() {
        return "statis + result : " + Person.staticMethodForMocks();
    }
}