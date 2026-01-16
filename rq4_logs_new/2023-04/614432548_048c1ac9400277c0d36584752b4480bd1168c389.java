package org.example.homework_nr_5;

enum MonthName {
    JANUARY, FEBRUARY, MARCH, APRIL, MAY, JUNE, JULY, AUGUST, SEPTEMBER, OCTOBER, NOVEMBER, DECEMBER,
}
    class MonthofYear {
        MonthName[] Month;

        public MonthofYear(MonthName[] month) {
            Month = month;
        }

        public void printAllMonthName() {
            int index;
            for (index = 0; index != Month.length; index++) {
                System.out.println(Month[index]);
            }
        }
    }
        public class HomeWork5 {
            public static void main(String[] args) {
//        Определите перечисление, в котором будут храниться месяцы календарного года.
//        Перебрать значения этого перечисления и вывести их на консоль.

                MonthofYear Monthtype = new MonthofYear (MonthName.values());
                Monthtype.printAllMonthName();

//5 Создайте перечисление с именем Weekday, в котором будут храниться дни недели: MONDAY , TESDAY … SUNDAY .
// Определите в перечислении два метода: isWeekDay() и isHoliday(), оба с типом возвращаемого значения boolean ,
// которые должны указывать, представляет ли значение в перечислении, в которое был вызван один из этих методов,
// будний или выходной день.

                System.out.println(Weekday.SUNDAY.isWeekday());
                System.out.println(Weekday.MONDAY.isWeekday());
                System.out.println(Weekday.FRIDAY.isWeekday());
                System.out.println(Weekday.TUESDAY.isHoliday());
                System.out.println(Weekday.SATURDAY.isHoliday());
                System.out.println("============================");

//  6. Преобразуйте следующие значения String в соответствующие примитивные типы данных:
                boolean booleanValue= Boolean.parseBoolean("true");
                byte byteValue = Byte.parseByte("120");
                short shortValue= Short.parseShort("32000");
                int InvValue =Integer.parseInt("147895632");
                long longValue= Long.parseLong("987654321987654321");
                double doubleValue =Double.parseDouble("4444.5d");
                float floatValue =Float.parseFloat("2354.56f");

                System.out.println(booleanValue);
                System.out.println(byteValue);
                System.out.println(shortValue);
                System.out.println(InvValue);
                System.out.println(longValue);
                System.out.println(doubleValue);
                System.out.println(floatValue);
            }
        }

