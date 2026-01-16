package md.methodstask;

public class Customer { 

    private String name;
    private String gen;
    private int age;
    public Customer(String name, String gen, int age) {
        this.name = name;
        this.gen = gen;

        if (age > 127 || age <1) {
            this.age = 0;
        } else {
            this.age = age;
        }

    }

    public void setAge(int age){
        this.age = age;

    }

    public void setName(String name){
        this.name = name;

    }

    public int getAge(){
        return this.age;

    }
    public String getName(){
        return this.name;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "name='" + name + '\'' +
                ", gen='" + gen + '\'' +
                ", age=" + age +
                '}';
    }
}





