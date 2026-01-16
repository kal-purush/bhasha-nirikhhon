import java.util.*;
import java.io.*;
public class EducationApp extends Student {
    public EducationApp(){
        super("null","null","null","null");
    }
    public static void main(String [] args){
        ArrayList<String> mathEx = new ArrayList<>();
        ArrayList<String> Answer = new ArrayList<>();
        ArrayList<Student> students = new ArrayList<>();
        Scanner uI = new Scanner(System.in);
        String level = "0";
        String fileNameStudents = "src\\Students.txt";
        String username = "";
        String password = "";
        String name = "";
        String yesNo = "";
        int index = 0;
        /*
         * You still have to make a loop that loops through to see if they hava an account
         * make sure to make a check to see if the user exist in the student array list
         */
        insertStudent(students,fileNameStudents);
        System.out.println(students.get(0).getName());
        System.out.println("Are you a new user?(yes or no)");
        yesNo = uI.nextLine();
        while (yesNo.equals("yes")) {
            /*
             * make a method that checks is the username or password is already taken
             */

            System.out.println("Please enter your new user name: ");
            username = uI.nextLine();
            System.out.println("Please enteryour new password: ");
            password = uI.nextLine();
            System.out.println("Please enter your name: ");
            name = uI.nextLine();
            Student obj = new Student(username, password, level, name);
            students.add(obj);
            try {
                /*
                 * This takes care of the problem of the Arraylist refreshing
                 */
                FileWriter myWriter = new FileWriter(fileNameStudents, true);
                BufferedWriter bW = new BufferedWriter(myWriter);
                while (index < students.size()) {
                    String studentLine = username + ";" + password + ";" + level + ";" + name;
                    /*
                     * "Line.separator" makes the writer print to next line .
                     */
                    myWriter.write(studentLine + System.getProperty("line.separator"));
                    index++;
                }
                myWriter.close();
            } catch (IOException e) {
                System.out.println("An error occurred.");
                /*
                 * printStackTrace pinpoints the error in the code
                 */
                e.printStackTrace();
            }
        }
        System.out.println(students.get(0).getName());
        if(yesNo.equals("no")){
            /*
             * This if statment will loop through untill the user gives a vaild username and password
             */
            boolean pass = false;
            while(pass) {
                System.out.println("please enter your username: ");
                username = uI.nextLine();
                System.out.println("Please enter your passowrd: ");
                password = uI.nextLine();
                for (int i = 0; i < students.size(); i++) {
                    if(students.get(i).getUserName().equals(username) && students.get(i).getPassword().equals(password)){
                        System.out.println("Welcome back " + students.get(i).getName());
                        pass = true;
                    }
                }
            }
        }
        System.out.println();
        System.out.println("Would you like to do some math now? (yes or no)");
        yesNo = uI.nextLine();
        while(yesNo.equals("yes")){
            /*
             * Pleas eneter the the code for the program here
             *
             * Create a menu  for the user to pick what they want to do
             * need 4> if statments that have loops through
             *
             *
             * create  method that checks of a answer is correct or not
             * create variable that hold the ammount of correct answers
             *    * you might need to cereate a new private varble in Student
             *    * maybe one for each txt file or something
             *    *
             *
             *
             *
             */
        }


    }
    public static void insertMathQuestions(ArrayList<String> mathEx, ArrayList<String> Answer, String filename) {
        try {
            /*
             * Think of scanner of blank notepads
             */
            FileReader fr = new FileReader(filename);
            Scanner sC = new Scanner(fr);
            /*
             * Makes blank notepad of txt file
             */
            while(sC.hasNext()){
                /*
                 *Instead of using buffred reader it is easier to use scanners
                 * to insert into an ArrayList
                 */
                String txt = sC.nextLine();
                /*
                 * txt catches the entire first line include the ";"
                 */
                Scanner sCT = new Scanner(txt);
                /*
                 * sCT is a new blank notepad the holds txt String
                 */
                sCT.useDelimiter(";"); // This is the same as .split()
                String mathExpression ="";
                String answer = "";
                String rate = "";
                mathExpression = sCT.next();
                mathEx.add(mathExpression);
                answer = sCT.next();
                Answer.add(answer);
            }
             sC.close();
        } catch(FileNotFoundException badFile){
            System.out.println("Trouble finding file");
        }  catch (ArrayIndexOutOfBoundsException toofar){
            System.out.println("index out of bonunds");
        }
    }
    public static void insertStudent(ArrayList<Student> students, String filename) {
        try {
            /*
             * Think of scanner of blank notepads
             */
            FileReader fr = new FileReader(filename);
            Scanner sC = new Scanner(fr);
            /*
             * Makes blank notepad of txt file
             */
            while(sC.hasNext()){
                /*
                 *Instead of using buffred reader it is easier to use scanners
                 * to insert into an ArrayList
                 */
                String txt = sC.nextLine();
                /*
                 * txt catches the entire first line include the ";"
                 */
                Scanner sCT = new Scanner(txt);
                /*
                 * sCT is a new blank notepad the holds txt String
                 */
                sCT.useDelimiter(";"); // This is the same as .split()
                String username ="";
                String password = "";
                String level = "";
                String name = "";
                username = sCT.next();
                password = sCT.next();
                level = sCT.next();
                name = sCT.next();
                Student obj = new Student(username,password,level,name);
                students.add(obj);
                /*
                 * This basically finds the next string in the line and insersts it into a String and makes
                 * a obj and then adds that into a Arraylist
                 */
            }
            sC.close();
        } catch(FileNotFoundException badFile){
            System.out.println("Trouble finding file");
        }  catch (ArrayIndexOutOfBoundsException toofar){
            System.out.println("index out of bonunds");
        }
    }
}