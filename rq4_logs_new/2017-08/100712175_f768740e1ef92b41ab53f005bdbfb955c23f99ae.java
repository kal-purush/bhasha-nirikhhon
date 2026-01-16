package ectsmonitor;

/**
 *
 * @author jor
 */

import java.util.*;    


public class ECTSMonitor {

    public static void main(String[] args) {
        Scanner stdin = new Scanner(System.in);
        
        //declaring the courses
        String Programming = "Programming";
        String Business = "Business";
        String Infrastructure = "Infrastructure";
        String Databases = "Databases";
        String UI = "User Interact";
        String FYS = "project FYS";
        String AD = "project PAD";
    
        //initializing the ects 
        int ectProgramming = 3;
        int ectBusiness = 3;
        int ectInfrastructure = 3;
        int ectDatabases = 3;
        int ectUI = 3;
        int ectFYS = 12;
        int ectAD = 12;
        int ectTotal;
        int ectMax = ectProgramming + ectBusiness + ectInfrastructure + 
                ectDatabases + ectUI + 
                ectFYS + ectAD;
        int ectRequired = (int)(ectMax /6 * 5);
        
        //input grade per course here
        System.out.print("your grade for " + Programming + ": " );
        double gradeProgramming = stdin.nextDouble();
        System.out.print("your grade for " + Business + ": " );
        double gradeBusiness = stdin.nextDouble();
        System.out.print("your grade for " + Infrastructure + ": " );
        double gradeInfrastructure = stdin.nextDouble();
        System.out.print("your grade for " + Databases + ": " );
        double gradeDatabases = stdin.nextDouble();
        System.out.print("your grade for " + UI + ": " );
        double gradeUI = stdin.nextDouble();
        System.out.print("your grade for " + FYS + ": " );
        double gradeFYS= stdin.nextDouble();
        System.out.print("your grade for " + AD + ": " );
        double gradeAD = stdin.nextDouble();
        
        //blank lines
        System.out.println("");
        System.out.println("");
        
        //calculate if ects have beeen achieved
        if (gradeProgramming >= 5.5) {ectProgramming = 3;}
        else {ectProgramming = 0;}
        if (gradeBusiness >= 5.5) {ectBusiness = 3;}
        else {ectBusiness = 0;}
        if (gradeInfrastructure >= 5.5) {ectInfrastructure = 3;}
        else {ectInfrastructure = 0;}
        if (gradeDatabases >= 5.5) {ectDatabases = 3;}
        else {ectDatabases = 0;}
        if (gradeUI >= 5.5) {ectUI = 3;}
        else {ectUI = 0;}
        if (gradeFYS >= 5.5) {ectFYS = 12;}
        else {ectFYS = 0;}
        if (gradeAD >= 5.5) {ectAD = 12;}
        else {ectAD = 0;}
        
        //print the course/grade/ect table
        System.out.println("course: " + Programming + '\t' +
                "Grade: " + gradeProgramming + '\t' +
                "ECTS achieved: " + ectProgramming);
        System.out.println("course: " + Business + '\t' +
                "Grade: " + gradeBusiness + '\t' +
                "ECTS achieved: " + ectBusiness);
        System.out.println("course: " + Infrastructure + '\t' +
                "Grade: " + gradeInfrastructure + '\t' +
                "ECTS achieved: " + ectInfrastructure);
        System.out.println("course: " + Databases + '\t' +
                "Grade: " + gradeDatabases + '\t' +
                "ECTS achieved: " + ectDatabases);
        System.out.println("course: " + UI + '\t' + 
                "Grade: " + gradeUI + '\t' + 
                "ECTS achieved: " + ectUI);
        System.out.println("course: " + FYS + " " + '\t' +
                "Grade: " + gradeFYS +  " " + '\t' +
                "ECTS achieved: " + ectFYS);
        System.out.println("course: " + AD + '\t' +
                "Grade: " + gradeAD + '\t' +
                "ECTS achieved: " + ectAD);
        
        //calculate average
        double average = (gradeProgramming + gradeBusiness + gradeInfrastructure + 
                gradeDatabases + gradeUI + 
                gradeFYS + gradeAD) / 7;
        int intAverage = (int)(average * 100);
        average = (double) intAverage / 100;
        System.out.println("");
        System.out.println("your average grade is:" + '\t' + average);
        
        
        //initialize ectTotal
        ectTotal = ectProgramming + ectBusiness + ectInfrastructure + 
                ectDatabases + ectUI + 
                ectFYS + ectAD;
       
        //print the amount of ects achiefed vs the max ects
        System.out.println("");
        System.out.println("Totaal behaalde ECTS: " + ectTotal + "/" + ectMax);
        
                
        //calculate if on ects schedule
        if (ectTotal < ectRequired){
            System.out.println("BE CAREFUL: you are behind on schedule");
        }
        else {
            System.out.println("CONGRATULATIONS: you are on schedule");
        }

                }
    }
    
}