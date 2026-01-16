import java.util.*;
import java.util.stream.Stream;

// p 391

public class App {
    public static void main(String[] args) throws Exception {
        /* 1.3 stream中间流操作 */

        /*
            1. create a arraylist
            2. add 6 different name into the list
            3. using filter  to get name start with "Z" (s.startsWith())
        */ 
        ArrayList<String> list = new ArrayList<>();
        list.add("Zelda");
        list.add("Zero");
        list.add("Moon");
        list.add("June");
        list.add("April");
        list.add("Monica");

        list.stream().filter(s -> s.startsWith("Z")).forEach(System.out::println);
        System.out.println("--------");

        /*
         * q1. 取前3个元素，输出
         */
        list.stream().limit(3).forEach(System.out::println);
        System.out.println("---------");

         /*
          * q2. 跳过3个元素， 输出剩下的元素
          */
        list.stream().skip(3).forEach(System.out::println);
        System.out.println("-------");

          /*
           * q3. 跳过2个，把剩下元素中的前2个输出
           */
          list.stream().skip(2).limit(2).forEach(System.out::println);
          System.out.println("-------");

          /*
           * q4. 取前4个数据组成一个流， 跳过前2个数据组成一个流， 合成一个新流
           * 去除重复值
           */
          Stream<String> s1 = list.stream().limit(4);
          Stream<String> s2 = list.stream().skip(2);
          Stream<String> s3 = Stream.concat(s1, s2);;
        //   s3.forEach(System.out::println);
          s3.distinct().forEach(System.out::println);
          System.out.println("-------");

          /* 
           * sorted(), sort element by it's first char;
           * sorted(Comparator comparator) 
          */
          list.stream().sorted().forEach(System.out::println);
          System.out.println("-------");

          list.stream().sorted((str1,str2) -> str1.length()-str2.length()).forEach(System.out::println);
          System.out.println("-------");

          list.stream().sorted((str1,str2) -> {
            int num1 = str1.length() - str2.length();
            int num2 = num1 == 0 ? str1.compareTo(str2):num1;
            return num2; 
          } ).forEach(System.out::println);
          System.out.println("-------");
    }
}