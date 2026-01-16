import java.util.ArrayList;
import java.util.Scanner;

public class Main3PartTwo {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        Contact con = new Contact();
        Education edu = new Education();
        Experience exp = new Experience();
        Skill sk1 = new Skill();

        //*********** GET INPUTS FROM USER ************
        System.out.println("Welcome to the Robo-Resume generator!" +
                "\nPlease put in the following information, and your completed resume will display below");

        //contact info
        System.out.println("\nCONTACT INFO");
        System.out.print("Enter your name: ");
        con.setName(sc.nextLine());
        System.out.print("Enter your email: ");
        con.setEmail(sc.nextLine());
        System.out.print("Enter your phone number: ");
        con.setPhone(sc.nextLine());

        //education
        String repeat = "";
        ArrayList<String> educationList = new ArrayList<>();
        System.out.println("\nEDUCATION");
        while(!repeat.equalsIgnoreCase("N")){
            System.out.println("Do you want to input another EDUCATION entry (Y/N)? ");
            repeat = sc.nextLine();

            if (repeat.equalsIgnoreCase("Y")){
                //get details from user
                System.out.print("Enter your degree type: ");
                edu.setDegree(sc.nextLine());
                System.out.print("Enter your major: ");
                edu.setMajor(sc.nextLine());
                System.out.print("Enter your university name: ");
                edu.setUniversity(sc.nextLine());
                System.out.print("Enter your graduation year: ");
                edu.setGradYear(sc.nextLine());

                //add to arraylist
                educationList.add(edu.toString());
            }
        }

        //experience
        repeat = "";
        ArrayList<String> experienceList = new ArrayList<>();
        System.out.println("\nWORK EXPERIENCE");
        while(!repeat.equalsIgnoreCase("N")){
            System.out.println("Do you want to input another WORK EXPERIENCE entry (Y/N)? ");
            repeat = sc.nextLine();

            if (repeat.equalsIgnoreCase("Y")){
                //get details from user
                System.out.print("Enter your company: ");
                exp.setCompany(sc.nextLine());
                System.out.print("Enter your job title: ");
                exp.setJobTitle(sc.nextLine());
                System.out.print("Enter your job description: ");
                exp.setDescription(sc.nextLine());
                System.out.print("Enter your start date: ");
                exp.setStartDate(sc.nextLine());
                System.out.print("Enter your end date: ");
                exp.setEndDate(sc.nextLine());

                //add to arraylist
                experienceList.add(exp.toString());
            }
        }

        //skills
        repeat = "";
        ArrayList<String> skillList = new ArrayList<>();
        System.out.println("\nSKILLS");
        while(!repeat.equalsIgnoreCase("N")){
            System.out.println("Do you want to input another SKILLS entry (Y/N)? ");
            repeat = sc.nextLine();

            if (repeat.equalsIgnoreCase("Y")){
                //get details from user
                System.out.print("Enter your skill name: ");
                sk1.setSkillName(sc.nextLine());
                System.out.print("Enter your proficiency \n(Fundamental, Novice, Intermediate, Advanced, Expert): ");
                sk1.setProficiency(sc.nextLine());

                //add to arraylist
                skillList.add(sk1.toString());
            }
        }

        System.out.println("\nWow, you are awesome! Take a look at your beautiful resume below:");



        //************ DISPLAY RESUME ************
        System.out.println("\n\n\n=============================================================");

        //display contact info
        System.out.println(con);

        //display education
        System.out.println("------ EDUCATION ------");
        for(String key: educationList){
            System.out.println(key);
        }

        //display experience
        System.out.println("------ EXPERIENCE ------");
        for(String key: experienceList){
            System.out.println(key);
        }

        //display skills
        System.out.println("------ SKILLS ------");
        for(String key: skillList){
            System.out.println(key);
        }


    }
}
