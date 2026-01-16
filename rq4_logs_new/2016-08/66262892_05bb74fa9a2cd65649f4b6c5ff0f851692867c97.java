/*****************************
 * COSC 1173 Programming Lab
 * Name: John Jenkins
 * Data: 8/23/2016
 * Display personal information
 *******************************/
public class Lab1 {

    public static void main(String[] args) {

        printData("John Jenkins", "Computer Science", "Coding");
    }

    /**
     * @printData display the personal info arguments passed from the main method
     * @param name String containing name
     * @param major String containing major
     * @param hobbies String containing hobbie
     */
    public static void printData(String name, String major, String hobbies)
    {
        System.out.println("Personal Information");//personal info header
        System.out.println("Name: "+name);//name label with concatenated name argument
        System.out.println("Major: "+major);//major label with concatenated major argument
        System.out.println("Hobbies: "+hobbies);//hobbies label with concatenated hobbies argument

    }
}