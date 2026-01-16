import java.util.Scanner;
import java.util.ArrayList;
public class SwitchClass {

 public static void switchClass() {

Scanner userInput = new Scanner(System.in);
for(int j = 0; j < StudentList.studentList.size(); j++)
{
System.out.println( j + 1 + ")" + StudentList.studentList.get(j).getFirstName() + " " + StudentList.studentList.get(j).getLastName() + " " + StudentList.studentList.get(j).getFirstClass()  + " " + StudentList.studentList.get(j).getFirstGrade() + " " + StudentList.studentList.get(j).getSecondClass() + " " + StudentList.studentList.get(j).getSecondGrade() + " " + StudentList.studentList.get(j).getThirdClass() + " " +  StudentList.studentList.get(j).getThirdGrade() + " ");
}

System.out.println("What students classes would you like to change");
int i = userInput.nextInt();




System.out.println(" What class would you like to move");
System.out.println("1)" + " " + StudentList.studentList.get(i).getFirstClass());
System.out.println("2)" + " " + StudentList.studentList.get(i).getSecondClass());
System.out.println("3)" + " " + StudentList.studentList.get(i).getThirdClass());
String first = StudentList.studentList.get(i).getFirstClass();
String second = StudentList.studentList.get(i).getSecondClass();
String third = StudentList.studentList.get(i).getThirdClass();
int swap = userInput.nextInt();
if(swap == 1)
{
System.out.println("what class do you want to switch with " + StudentList.studentList.get(i).getFirstName() + "'s " + StudentList.studentList.get(i).getFirstClass());
System.out.println("2)" + " " + StudentList.studentList.get(i).getSecondClass());
System.out.println("3)" + " " + StudentList.studentList.get(i).getThirdClass());
int changeFirst = userInput.nextInt();
if (changeFirst == 2)
{
StudentList.studentList.get(i).setFirstClass(second);
StudentList.studentList.get(i).setSecondClass(first);
}
if (changeFirst == 3)
{
StudentList.studentList.get(i).setFirstClass(third);
StudentList.studentList.get(i).setThirdClass(first);
}
System.out.println( i + ")" + StudentList.studentList.get(i).getFirstName() + " " + StudentList.studentList.get(i).getLastName() + " " + StudentList.studentList.get(i).getFirstClass()  + " " + StudentList.studentList.get(i).getFirstGrade() + " " + StudentList.studentList.get(i).getSecondClass() + " " + StudentList.studentList.get(i).getSecondGrade() + " " + StudentList.studentList.get(i).getThirdClass() + " " +  StudentList.studentList.get(i).getThirdGrade() + " ");


}
if(swap == 2)
{
System.out.println("what class do you want to switch with " + StudentList.studentList.get(i).getFirstName() + "'s " + StudentList.studentList.get(i).getSecondClass());
System.out.println("1)" + " " + StudentList.studentList.get(i).getFirstClass());
System.out.println("3)" + " " + StudentList.studentList.get(i).getThirdClass());
int change2 = userInput.nextInt();
if (change2 == 1)
{
StudentList.studentList.get(i).setFirstClass(second);
StudentList.studentList.get(i).setSecondClass(first);
}
if (change2 == 3)
{
StudentList.studentList.get(i).setSecondClass(third);
StudentList.studentList.get(i).setThirdClass(second);
}
System.out.println( i + ")" + StudentList.studentList.get(i).getFirstName() + " " + StudentList.studentList.get(i).getLastName() + " " + StudentList.studentList.get(i).getFirstClass()  + " " + StudentList.studentList.get(i).getFirstGrade() + " " + StudentList.studentList.get(i).getSecondClass() + " " + StudentList.studentList.get(i).getSecondGrade() + " " + StudentList.studentList.get(i).getThirdClass() + " " +  StudentList.studentList.get(i).getThirdGrade() + " ");


}
if(swap == 3)
{
System.out.println("what class do you want to switch with " + StudentList.studentList.get(i).getFirstName() + "'s " + StudentList.studentList.get(i).getThirdClass());
System.out.println("1)" + " " + StudentList.studentList.get(i).getFirstClass());
System.out.println("2)" + " " + StudentList.studentList.get(i).getSecondClass());
int change3 = userInput.nextInt();
if (change3 == 1)
{
StudentList.studentList.get(i).setThirdClass(first);
StudentList.studentList.get(i).setFirstClass(third);
}
if (change3 == 2)
{
StudentList.studentList.get(i).setThirdClass(second);
StudentList.studentList.get(i).setSecondClass(third);
}
System.out.println( i + ")" + StudentList.studentList.get(i).getFirstName() + " " + StudentList.studentList.get(i).getLastName() + " " + StudentList.studentList.get(i).getFirstClass()  + " " + StudentList.studentList.get(i).getFirstGrade() + " " + StudentList.studentList.get(i).getSecondClass() + " " + StudentList.studentList.get(i).getSecondGrade() + " " + StudentList.studentList.get(i).getThirdClass() + " " +  StudentList.studentList.get(i).getThirdGrade() + " ");


}


}

}