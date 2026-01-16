package app;

import collection.CollectionManager;
import console.CommandManager;
import console.ConsoleManager;
import fileManager.Parser;

import javax.xml.bind.JAXBException;
import java.io.File;

/**
 * Главный класс программы.
 * Запускает интерактивный режим и загружает коллекцию из XML-файла.
 */
public class Main {
    public static void main(String[] args) {
        CollectionManager collectionManager = new CollectionManager();
        ConsoleManager consoleManager = new ConsoleManager();

        try {
            // Получаем имя файла из переменной окружения
            String filename = System.getenv("FILENAME");

            // Проверяем, передано ли имя файла
            if (filename == null || filename.isEmpty()) {
                System.out.println("Ошибка: Переменная окружения 'FILENAME' не установлена.");
                return;
            }

            File file = new File(filename);

            if (file.exists() && !file.isDirectory()) {
                // Загружаем коллекцию из XML-файла
                Parser parser = new Parser(filename);
                collectionManager.setCollection(parser.loadFromXml());

                // Создаем менеджер команд
                CommandManager commandManager = new CommandManager(collectionManager);
                commandManager.setFilename(filename);
                collectionManager.setFilename(filename);

                // Запускаем интерактивный режим
                while (commandManager.getWork()) {
                    commandManager.existCommand();
                }
            } else {
                System.out.println("Ошибка: Файл '" + filename + "' не найден или не доступен.");
                consoleManager.fileRead();
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Ошибка ввода данных: " + e.getMessage());
            consoleManager.fileRead();
        } catch (JAXBException ex) {
            System.out.println("Ошибка обработки XML-файла: " + ex.getMessage());
        } catch (Exception e) {
            System.out.println("Что-то пошло не так. Попробуйте снова. " + e.getMessage());
        }
    }
}