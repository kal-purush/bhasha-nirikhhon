import java.util.*;
import java.util.stream.Stream;

// p 391

public class StreamLearn {
    public static void main(String[] args) throws Exception {
        /* 1.3 stream中间流操作 */
        ArrayList<String> nums = new ArrayList<>();
        nums.add("10");
        nums.add("9");
        nums.add("6");
        nums.add("2");
        nums.add("11");

        /*
            1. create a arrayna me
            2. add 6 different name into the list
            3. using filter  to get name start with "Z" (s.startsWith())
        */ 
        ArrayList<String> name = new ArrayList<>();
        name.add("Zelda");
        name.add("Zero");
        name.add("Moon");
        name.add("June");
        name.add("April");
        name.add("Monica");

        name.stream().filter(s -> s.startsWith("Z")).forEach(System.out::println);
        System.out.println("--------");

        /*
         * q1. 取前3个元素，输出
         */
        name.stream().limit(3).forEach(System.out::println);
        System.out.println("---------");

         /*
          * q2. 跳过3个元素， 输出剩下的元素
          */
        name.stream().skip(3).forEach(System.out::println);
        System.out.println("-------");

          /*
           * q3. 跳过2个，把剩下元素中的前2个输出
           */
          name.stream().skip(2).limit(2).forEach(System.out::println);
          System.out.println("-------");

          /*
           * q4. 取前4个数据组成一个流， 跳过前2个数据组成一个流， 合成一个新流
           * 去除重复值
           */
          Stream<String> s1 = name.stream().limit(4);
          Stream<String> s2 = name.stream().skip(2);
          Stream<String> s3 = Stream.concat(s1, s2);;
        //   s3.forEach(System.out::println);
          s3.distinct().forEach(System.out::println);
          System.out.println("-------");

          /* 
           * sorted(), sort element by it's first char;
           * sorted(Comparator comparator) 
          */
          name.stream().sorted().forEach(System.out::println);
          System.out.println("-------");

          name.stream().sorted((str1,str2) -> str1.length()-str2.length()).forEach(System.out::println);
          System.out.println("-------");

          name.stream().sorted((str1,str2) -> {
            int num1 = str1.length() - str2.length();
            int num2 = num1 == 0 ? str1.compareTo(str2):num1;
            return num2; 
          } ).forEach(System.out::println);
          System.out.println("-------");

          /*
           * convert a Stream to IntStream
           */
          // return a stream
          nums.stream().map(Integer::parseInt).forEach(System.out::println);
          // return a IntStream, which has sum() method
          nums.stream().mapToInt(Integer::parseInt).forEach(System.out::println);
          int result = nums.stream().mapToInt(Integer::parseInt).sum();
          System.out.println("sum: " + result);

         /*
          * count the number of name which start with "z"
          */
          long count = name.stream().filter(n -> n.startsWith("Z")).count();
          System.out.println("count:" + count);

    }
}