package Lesson030_RegularExpressionsPart1;


    public class Test {
        public static void main(String[] args) {
        /*
        \\d - одна цифра
        \\w -  одна английская буква = [a-zA-Z]
        \\d+ - 1 и более
        \\d* - 0 и более
        ? - 0 или 1 символов до
        ( x|y|z ) - одна строка из множества строк
        (a|b|c|d|e|f|g) --> [a-g]
        [a-zA-Z] - все английские буквы
        [0-9] - \\d
        [^abc] - все символы, кроме a,b,c
        . - любой символ
        {2} - 2 символа до (\\d{2})
        {2, } - 2 или более символа (\\d{2, }
        {2, 4} - от 2 до 4 символов (\\d{2,4}
        regexlib.com/CheatSheet.aspx  --- сайт с регулярными выражениями

        */
            String a = "d";
            String b = "41241";
            String c = "+12124";
//        String d = "-41241";

            System.out.println(a.matches("\\d"));
            System.out.println(a.matches("\\d"));
            System.out.println(a.matches("\\d"));
//        System.out.println(d.matches("(-|\\+)?\\d"));

            String d = "gsodrig331111jsdvm1354251";
            System.out.println(d.matches("[a-zA-Z31]+\\d+"));

            String e = "hello";
            System.out.println(e.matches("[^abc]*"));

            String url = "http://www.google.com";
            String url2 = "http://www.yandex.ru";
            String url3 = "Hello there!";
            System.out.println(url.matches("http://www\\..+\\.(com|ru)"));
            System.out.println(url2.matches("http://www\\..+\\.(com|ru)"));
            System.out.println(url3.matches("http://www\\..+\\.(com|ru)"));

            String f = "123";
            System.out.println(f.matches("\\d{2}"));


        }

    }

