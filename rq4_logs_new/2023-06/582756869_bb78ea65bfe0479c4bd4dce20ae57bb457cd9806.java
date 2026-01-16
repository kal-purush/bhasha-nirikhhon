public class Task_1 {
    public static String str = "Циклом называется многократное выполнение одних и тех же действий.";

    public static int countLetters(String str, int a) {
        return (str.substring(0, a).replace(" ", "").length());
    }

    public static void main(String[] args) {
         int a = str.indexOf(args[0]);
        System.out.println(countLetters(str, a));
    }


}