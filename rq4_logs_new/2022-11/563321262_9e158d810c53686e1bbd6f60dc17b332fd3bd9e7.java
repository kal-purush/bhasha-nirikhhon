import java.util.MissingFormatArgumentException;
import java.util.Date;
import java.math.BigInteger;
import java.util.Scanner;

public class Main {
/*     public static void main(String[] args) {
          System.out.println("First element");
      }
  }
 */
    //Задача №1
   /* public static void main(String[] args) {
int a = 6;
int b = 4;
int Sum = a+b;
int Min = a-b;
int Seg = a/b;
int Mul = a*b;
int Rem = a%b;
int Per = a*100/b;
        System.out.println("Сумма:"+Sum);
        System.out.println("Разность:"+Min);
        System.out.println("Произведение:"+Seg);
        System.out.println("Деление:"+Mul);
        System.out.println("Остаток от деления:"+Rem);
        System.out.println("Процент от числа:"+Per+"%");
    }
}
*/
    //Задача №2
    /*
    public static void main(String[] args) {
        int Calcium = 12;
        int Fahrenheit = (Calcium * 9/5) + 32;
        System.out.println(Fahrenheit+"°F");
    }
}
*/
    //Задача №3
    /*
    public static void main(String[] args) {
        Date date = new Date();
        System.out.println(date);
    }
}
     */
    //Задача №4
    /*
    public static void main(String[] args) {
        int a = 15;
        int b = 12;
        if (a > b)
            System.out.println(a);
        else
            System.out.println(b);
    }
}
     */
    //Задача №5
 /*
    public static void main(String[] args) {
int a = 21;
int b = 19;
int c = 16;
if (a > b && a > c)
    System.out.println(a);
if (b > a && b > c)
    System.out.println(b);
if (c > a && c > b)
    System.out.println(c);
    }
}
  */
    //Задача №6
    public static void main(String[] args) {
        int a = (int) (Math.random() * 100);
        int b = (int) (Math.random() * 100);
        int c = (int) (Math.random() * 100);
        System.out.println("Первое случайное число:"+a);
        System.out.println("Второе случайное число:"+b);
        System.out.println("Третье случайное число:"+c);
    }
}
 /*
    //Задача №7
public static void main(String[] args) {
    Scanner in = new Scanner(System.in);
    System.out.println("Введите год:");
    int Year = in.nextInt();
    int Century = Year / 100 + 1;
    System.out.println(Century+" Век");
}
}
  */