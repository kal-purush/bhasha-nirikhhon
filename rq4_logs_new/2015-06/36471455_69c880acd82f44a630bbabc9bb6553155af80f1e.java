/**
 * Created by amyquispe on 6/2/15.
 */
public class MyObject {
    String name;
    int age;

    MyObject() {
        name = null;
        age = 0;
    }

    MyObject(String newName, int newAge){
        this.name = newName;
        this.age = newAge;
    }

    public void setName(String newName){
        name = newName;
    }

    public void setAge(int newAge){
        age = newAge;
    }

    public String getName(){
        return name;
    }

    public int getAge(){
        return age;
    }

    public boolean equals(MyObject other){
        return (age == other.getAge() && name.equals(other.getName()));
    }

}