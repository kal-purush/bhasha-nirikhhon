package List;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class One {
    String a;

    public One(String a) {
        this.a = a;
    }

    List<String> list=new ArrayList<>();
    void hometask_1(){
        //синя основа
        boolean i=true;
        while(i=true){

            String name="break";
            System.out.println("Напиши слово:");
            Scanner scan=new Scanner(System.in);
            String word=scan.next();


            System.out.println(word);
            System.out.println(name);



            if( word!=name){
                System.out.println("hash word "+" "+word.hashCode()+"hash name"+" "+name.hashCode());
                System.out.println(list.add(word));
                System.out.println(list.toString());
            }else if (word.hashCode()==name.hashCode()){
                //що пробував у else if  ставити:
                //word==name    word=="break"
                System.out.println("Ви ввели слово break");
                break;
            }else {
                System.out.println("якась фігня");
            }
        }

        //потім видалити коли вирішу проблему із break,тому шо вже не треба  вручну додавання бо буде scanner
        list.add("sonicc");
        list.add("so");
        list.add("sic");
        list.add("c");
        list.add("o");

    }




    //2 зелений підпункт
    void hometask_2(){
        Pattern pattern=Pattern.compile("^s");

        for (String s : list) {
            Matcher matcher=pattern.matcher(s);
            if (matcher.find()==true){
                System.out.println("Слова які починаються на  [s]:"+" "+s);
            }
        }
    }


    //3 зелений підпункт
    void hometask_3(){
        for (String s : list) {

            if (s.length()>5){
                System.out.println("Завдання про ширину строки якщо більше ніж 5 то виводити:"+" "+s);
            }

        }
        //1 зелений підпункт
        System.out.println("Весь масив:"+" "+list.toString());
    }







    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        One one = (One) o;
        return Objects.equals(a, one.a);
    }

    @Override
    public int hashCode() {
        return Objects.hash(a);
    }

    @Override
    public String toString() {
        return "One{" +
                "a='" + a + '\'' +
                '}';
    }

}