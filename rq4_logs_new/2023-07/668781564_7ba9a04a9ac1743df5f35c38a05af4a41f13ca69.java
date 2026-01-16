public class Person {
//Write a class with the name Person. The class needs three fields
// (instance variables) with the names firstName, lastName of type
// String and age of type int.
    String firstName;
    String lastName;
    int age;

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getAge() {
        return age;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public boolean isTeen(){
        if (( 12<this.age)&(this.age<20)){
            return true;
        } else {
            return false;
        }
    }

    public String getFullName(){
        if(firstName.isEmpty()){
            firstName = "";
        }
        if(lastName.isEmpty()){
            lastName = "";
        }
        String a = firstName+' '+lastName;
        return a;
    }

    public static void main(String[] args) {
        Person person = new Person();
        person.setFirstName("nee");
        person.setLastName("Daa");
        person.setAge(16);
        boolean n = person.isTeen();
        person.getFullName();
        System.out.println(person.getFullName());
        System.out.println("fullName= " + person.getFullName());
	    System.out.println("teen= " + person.isTeen());
	    person.setFirstName("John");    // firstName is set to John
	    person.setAge(18);
	    System.out.println("fullName= " + person.getFullName());
	    System.out.println("teen= " + person.isTeen());
	    person.setLastName("Smith");    // lastName is set to Smith
	    System.out.println("fullName= " + person.getFullName());
    }
}


