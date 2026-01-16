//***********************//
//****Program author*****//
//******Te3Ka_PaynE******//
//*79811131773@yandex.ru*//
//***********************//

package ru.te3ka_programm;


/**
 * Класс декоратора.
 * Отображает большую часть текста в консоли.
 * Также содержит константы для цвета текста в консоли.
 */
class Decorator {
    // Цвет консольных команд
    final String ANSI_RESET = "\u001B[0m";
    final String ANSI_YELLOW = "\u001B[33m";
    final String ANSI_RED = "\u001B[31m";
    final String ANSI_PURPLE = "\u001B[35m";
    final String ANSI_GREEN = "\u001B[32m";


    /**
     * Метод для показа консольного текста с описанием работы программы.
     */
    void startProgram() {
        System.out.println("Программа показывает инкрементируемый счётчик.");
        System.out.println("При выходе из приложения показатель счётчика сохраняется.");
        System.out.println("При повторном запуске счётчик загружается с последнего сохранённого положения.");
        System.out.println();
    }

    /**
     * Метод для показа консаольного текста с описанием команд для работы программы.
     */
    void showCommandsMenu() {
        System.out.println("Команды управления программой:");
        System.out.println("-- " + ANSI_YELLOW + Logic.PLUS_ITERATOR + ANSI_RESET + " - увеличение счётчика на 1;");
        System.out.println("-- " + ANSI_YELLOW + Logic.RESET_ITERATOR + ANSI_RESET + " - сброс счётчика до 0.");
        System.out.println("-- " + ANSI_YELLOW + Logic.STOP_ITERATOR + ANSI_RESET + " - завершить работу программы.");
        System.out.println("Примечание: команды управления нужно вводить на английской раскладке маленькими буквами и указанием знака \"/\" в начале команды.");
        System.out.print("Введите команду >>: ");
    }

    /**
     * Метод для отображения текущего значения итератора в консоли.
     * @param iterator - счётчик
     */
    void showIterator(Iterator iterator) {
        System.out.println("Текущее значение счётчика: " + ANSI_RED + iterator.getIterator() + ANSI_RESET);
    }

    /**
     * Метод для показа консольного текста автора программы.
     */
    void author() {
        System.out.println();
        System.out.println(ANSI_YELLOW + "/" + "*".repeat(23) + "/");
        System.out.println("/****" + ANSI_PURPLE + "Program author" + ANSI_YELLOW + "*****/");
        System.out.println("/******" + ANSI_PURPLE + "Te3Ka_PaynE" + ANSI_YELLOW + "******/");
        System.out.println("/*" + ANSI_PURPLE + "79811131773@yandex.ru" + ANSI_YELLOW + "*/");
        System.out.println("/" + "*".repeat(23) + "/" + ANSI_RESET);
    }
}