package grades;

import java.util.HashMap;
import java.util.Random;
import util.Input;

public class GradesApplication {
    public static void main(String[] args) {
        // <github username, Student object>
        HashMap<String, Student> students = new HashMap<>();

        // Create 4 Students
        Student johnny = new Student("Johnny");
        Student alison = new Student("Ali");
        Student daniel = new Student("Daniel");
        Student tommy = new Student("Tommy");

        //Assign grades to each Student object
        assignGrades(johnny);
        assignGrades(alison);
        assignGrades(daniel);
        assignGrades(tommy);

        //Add Students and their usernames to Hashmap students
        students.put("cobrakai818", johnny);
        students.put("badboymagnet", alison);
        students.put("MiyagiDo862", daniel);
        students.put("nomercy", tommy);

        initialGreeting(students);

    }

    public static void assignGrades(Student name){
        for (int i = 0; i <= 3 ; i++) name.addGrade(getRandomNumberUsingNextInt());
    }

    public static int getRandomNumberUsingNextInt() {
        Random random = new Random();
        return random.nextInt(31) + 70;
    }

    public static void initialGreeting(HashMap<String, Student> data) {
        System.out.println("Greetings!");
        System.out.println("These are the usernames of our current students.");
        for (String i : data.keySet()) {
            System.out.printf("|  %s  |", i);
        }
    }

    public static void displayInfo(){
        System.out.print("\nWhich student's info would you like to see? ");
        Input choice = new Input();
        choice.getString();
    }
}