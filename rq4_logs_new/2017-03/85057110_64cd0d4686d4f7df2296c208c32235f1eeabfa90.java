package MultipleImplementation;


public class Citizen implements Person, Birthable, Identifiable{
    private String name;
    private int age;
    private String id;
    private String birthdate;

    public Citizen(String name, Integer age, String id, String birthdate){
        this.setName(name);
        this.setAge(age);
        this.setId(id);
        this.setBirthdate(birthdate);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setBirthdate(String birthdate) {
        this.birthdate = birthdate;
    }

    @Override
    public String getName(){
        return this.name;
    }

    @Override
    public int getAge(){
        return this.age;
    }

    @Override
    public String getBirthdate(){
        return this.birthdate;
    }

    @Override
    public String getId(){
        return this.id;
    }
}