import java.util.Arrays;

public class ArraysExercises {
    public static void main(String[] args) {
        int[] numbers = {1, 2, 3, 4, 5};
        //Prints array location; not elements
        System.out.println(numbers);

        Person[] people = {new Person("James"), new Person("Bones"), new Person("Spock")};
        for(Person p: people){
            System.out.println(p.getName());
        }

        Person scott = new Person("Scotty");
        Person[] peopleAdded = addPerson(people, scott);
        for(Person p: peopleAdded){
            System.out.println(p.getName());
        }
    }
    public static Person[] addPerson(Person[] people, Person name){
        Person[] peopleAdded = Arrays.copyOf(people, people.length+1);
        peopleAdded[people.length] = name;
        return peopleAdded;
    }



}