import java.lang.NumberFormatException;
import java.io.IOException;
import java.util.Scanner;
public class Main {
    public static void main(String[]args) throws IOException {
        System.out.println("""
                Программа калькулятор работает только с целыми числами от 1 до 10 включительно.
                 Калькулятор умеет выполнять операции сложения, вычитания, умножения и деления с двумя числами.
                """);
        Scanner keyboard = new Scanner(System.in);                                               //
        boolean flag = true;                                                                    //  Метод calc поместил в цикл с проверкой на срабатывание выхода из цикла
        while (flag){                                                                          //   Что бы улучшить удобство пользования калькулятора
            System.out.print("Введите пример в одну строку\n"+"для выхода нажать '0'  : ");   //
            String input = keyboard.next();
            if (input.equals("0")){
                flag = false;
            }else {
                System.out.println("Ответ:  "+calc(input));
            }
        }
    }
    public static String calc(String input) throws IOException {
        String[] example = input.split("[*/+-]");
        if (example.length>2) throw new IOException("throws Exception //т.к. формат математической операции не удовлетворяет заданию - два операнда и один оператор (+, -, /, *)");
        else if (example.length<2) throw new IOException("throws Exception //т.к. строка не является математической операцией");
        String oneNum = example[0];       // первый оперен
        String twoNum = example[1];       // второй оперен
        int countTest =0;                 // счетчик для определения римских чисел в двух операндах
        int IntResulNumberRoma = 0;       // арабский эквивалент римского числа
        String resultateRoma="";          // результат полученного значения в римских числах
        Roma [] resul = Roma.needNumbers();                            //  массив для определения римских чисел по условию условия задания (от 1 до 10)
        for (Roma d : resul) {                                         //  при помощи цикла увеличиваю счетчик на 1 если введенный оперен присутствует в массиве
            if (d.romNum().equals(oneNum)) countTest++;                //
        }                                                              //
        for (Roma n:resul){                                            //
            if (n.romNum().equals(twoNum)) countTest++;                //
        }                                                              //    таким образом если счетчик == 2 то оба оперена принадлежат римским числам,
        switch (countTest) {                                           //    а значит: - при помощи.contains определяю знак оператора, и сразу же выполняю нужное действие,
            case 2 -> {                                                                                                         //  записывая его в IntResulNumberRoma.
                if (input.contains("+")) IntResulNumberRoma = Roma.valueOf(oneNum).arabNum() + Roma.valueOf(twoNum).arabNum();   //
                else if (input.contains("-"))
                    IntResulNumberRoma = Roma.valueOf(oneNum).arabNum() - Roma.valueOf(twoNum).arabNum();
                else if (input.contains("*"))
                    IntResulNumberRoma = Roma.valueOf(oneNum).arabNum() * Roma.valueOf(twoNum).arabNum();
                else if (input.contains("/"))
                    IntResulNumberRoma = Roma.valueOf(oneNum).arabNum() / Roma.valueOf(twoNum).arabNum();
            }
            case 1 ->                                                                                                             //  если счетчик равен 1, значит один из опер ендов принадлежит
                    throw new IOException("throws Exception //т.к. используются одновременно разные системы счисления");          //  Другой системы счисления. Выбрасываю исключение.
            case 0 -> {                                                                                                           //
                try {                                                                                                             //   если же countTest не изменился и == 0, то во введенных
                    Integer.parseInt(example[0]);                                                                                 //    О значениях находятся арабские цифры либо другие символы кроме
                    Integer.parseInt(example[1]);                                                                                 //   Римских "литералов". По этому Нужно проверить на наличие в них
                } catch (NumberFormatException e) {                                                                               //   Int типа данных. Для этого выбрасываю исключение если
                    System.out.println("throws Exception //т.к. строка не является математической операцией");                     //   привести к int не получилось.
                }                                                                                                               //    Затем проверяю на условие (от 1 до 10) теперь уже арабских цифр
                if ((Integer.parseInt(oneNum) < 1 || Integer.parseInt(oneNum) > 10) ||                                           //   и выбрасываю исключение если оперенды не соответствуют
                        (Integer.parseInt(twoNum) < 1 || Integer.parseInt(twoNum) > 10))                                         //  указаным условиям.
                    throw new IOException("Калькулятор должен принимать на вход числа от 1 до 10 включительно, не более");        //
                else {                                                                                                            //
                    if (input.contains("+")) return String.valueOf(Integer.parseInt(oneNum) + Integer.parseInt(twoNum));        //   После этого так же при помощи метода.contains определяю
                    else if (input.contains("-"))                                                                                   //  оператор и полученный результат сразу возвращаю.
                        return String.valueOf(Integer.parseInt(oneNum) - Integer.parseInt(twoNum));                             //
                    else if (input.contains("*"))
                        return String.valueOf(Integer.parseInt(oneNum) * Integer.parseInt(twoNum));
                    else if (input.contains("/"))
                        return String.valueOf(Integer.parseInt(oneNum) / Integer.parseInt(twoNum));
                }
            }
        }
        Roma [] boxEnumNambers = Roma.returnNumber();                   //  Когда результат арифметических действий с арабскими цифрами
        for (Roma res : boxEnumNambers) {                               //  готов то можно вернуть ответ с римскими
            if (res.arabNum() == IntResulNumberRoma) {                 //   при помощи цикла, перебираю полученное значение ранее IntResulNumberRoma
                resultateRoma = res.romNum();                         //    В значениях массива. При совпадении записываю результат римского
            }                                                          //   аналога в готовый результат который возвращаю после обработки
        }                                                               //  исключения в нем на отрицательные числа.
        if (resultateRoma.equals("")){                                  //
            try {                                                      //
                Integer.parseInt(resultateRoma);                        //
            } catch (NumberFormatException e) {                             //
                return "throws Exception //т.к. в римской системе нет отрицательных чисел";     //
                }
        }
        return resultateRoma;                                                                //
    }
}