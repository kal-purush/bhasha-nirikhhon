/******************************************************************************

                            Online Java Compiler.
                Code, Compile, Run and Debug java program online.
Write your code in this editor and press "Run" button to execute it.

*******************************************************************************/
 class Juet{
    String name;
    int age;

 String getName(){
    return name;
}
 void setName(String N){
    this.name=N;;
}
int getAge(){
    return age;
}
 void setage(){
    this.age=age;
}
} 
public class Main{
public static void main(String[] args){
    Juet m1=new Juet();
    m1.setName("Shreyaansh");
    m1.setAge(21);
    System.out.println(m1.getName());
    System.out.println(m1.getAge());
    }
}