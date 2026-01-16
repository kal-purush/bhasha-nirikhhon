package Memory_homework;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class One {
    void hometask_1() {
        String array[] = new String[2];
        String name;
        Pattern pattern=Pattern.compile("^a");

        for (int i = 0; i <= 1; i++) {
            System.out.println("Введи перше слово:");
            Scanner scanner1 = new Scanner(System.in);
            name = scanner1.nextLine();
            Matcher mat=pattern.matcher(name);

            if (mat.find()==true){
                array[i]=name;
            }else {
                array[i]="1";
            }

        }
        for (int a=0;a<=1;a++){
            System.out.println(array[a]);
        }

    }


    void hometask_2() {
        String array[] = new String[2];
        String name;
        Pattern pattern=Pattern.compile("@gmail.com$");

        for (int i = 0; i <= 1; i++) {
            System.out.println("Введи email:");
            Scanner scanner1 = new Scanner(System.in);
            name = scanner1.nextLine();
            Matcher mat=pattern.matcher(name);

            if (mat.find()==true){
                array[i]=name;
            }else {
                array[i]="1";
            }

        }
        for (int a=0;a<=1;a++){
            System.out.println(array[a]);
        }

    }
    void hometask_3(){
        String str="I love Java more than my friend. Java is so beautiful.";
        Pattern pattern1=Pattern.compile("Java");
        Matcher mat1=pattern1.matcher(str);
        System.out.println("Провіряєм чи в нашій стрічці є слово---Java:"+" "+mat1.find());
        String str_changes=str.replaceFirst("Java","C#");
        System.out.println("Замінили Java на С#:"+" "+str_changes);
        String str_changes2=str.replaceAll("Java","C#");
        System.out.println("Змінили всі Java на С#:"+" "+str_changes2);


    }
}