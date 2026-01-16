package GeekBrians.Slava_5655380.Homework.Lesson8;

import static GeekBrians.Slava_5655380.Util.*;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class FileManager {
    public static void main(String[] args) throws FileNotFoundException, IOException {
        task2();
        task3();
        task4();
    }

    private static void task2() throws IOException {
        String firstHalfPath = "Homeworks/src/GeekBrians/Slava_5655380/Homework/Lesson8/filesForTesting/firstHalf.txt",
                secondHalfPath = "Homeworks/src/GeekBrians/Slava_5655380/Homework/Lesson8/filesForTesting/secondHalf.txt",
                unitedFilePath = "Homeworks/src/GeekBrians/Slava_5655380/Homework/Lesson8/filesForTesting/unitedFile.txt";
        // ЗАДАНИЕ 1. Создать 2 текстовых файла, примерно по 50-100 символов в каждом(особого значения не имеет);
        fillFileWithNumbers(firstHalfPath, 50, 1);
        fillFileWithNumbers(secondHalfPath, 100, 51);

        // ЗАДАНИЕ 2. Написать метод, «склеивающий» эти файлы, то есть вначале идет текст из первого файла, потом текст из второго
        append(firstHalfPath, secondHalfPath, unitedFilePath);
    }

    private static void task3() throws IOException {
        System.out.println("\nЗАДАНИЕ 3");
        String word = "Lorem";
        // ЗАДАНИЕ 3.* Написать метод, который проверяет присутствует ли указанное пользователем слово в файле (работаем только с латиницей).
        Vector3i wordIndex = findWord(new File("Homeworks/src/GeekBrians/Slava_5655380/Homework/Lesson8/filesForTesting/FishTextA.txt"), word);
        if (wordIndex.x == -1) {
            System.out.println("Слово \"" + word + "\" не найдено");
            return;
        }
        System.out.println("Слово \"" + word + "\" найдено");
        System.out.println("Строка: " + wordIndex.y + ", Символ от начала строки: " + wordIndex.x);
    }

    private static void task4() throws IOException {
        System.out.println("\nЗАДАНИЕ 4");
        String word = "laboris";
        StringBuilder matchFilePathBuff = new StringBuilder();
        // ЗАДАНИЕ 4.** Написать метод, проверяющий, есть ли указанное слово в папке
        Vector3i wordIndex = findWord(new File("Homeworks/src/GeekBrians/Slava_5655380/Homework/Lesson8/filesForTesting"), word, matchFilePathBuff);
        if (wordIndex.x == -1) {
            System.out.println("Слово \"" + word + "\" отсутстует в Homeworks/src/GeekBrians/Slava_5655380/Homework/Lesson8/filesForTesting");
            return;
        }
        System.out.println("Слово \"laboris\" найдено в " + matchFilePathBuff);
        System.out.println("Строка: " + wordIndex.y + ", Символ от начала строки: " + wordIndex.x);
    }

    public static void fillFileWithNumbers(String path, int length, int startNumber) throws FileNotFoundException, IOException {
        FileOutputStream fos = new FileOutputStream(path, false);
        String content = "-";
        for (int i = startNumber; i < length + startNumber; i++) {
            content += i + "-";
            if (i % 25 == 0)
                content += "\n";
        }
        fos.write(content.getBytes(StandardCharsets.UTF_8));
        fos.close();
    }

    public static void append(String srcPath, String destPath) throws FileNotFoundException, IOException {
        FileOutputStream fos = new FileOutputStream(destPath, true);
        FileInputStream fis = new FileInputStream(srcPath);
        fos.write(fis.readAllBytes());
        fos.close();
        fis.close();
    }

    public static void append(String firstHalfPath, String secondHalfPath, String unitedFilePath) throws FileNotFoundException, IOException {
        File unitedFile = new File(unitedFilePath);
        if (unitedFile.exists())
            unitedFile.delete();
        unitedFile.createNewFile();
        append(firstHalfPath, unitedFilePath);
        append(secondHalfPath, unitedFilePath);
    }

    public static Vector3i findWord(String text, String word) {
        if (text.indexOf(word) == -1)
            return new Vector3i(-1, -1, -1);
        byte[] textChars = text.getBytes(),
               wordChars = word.getBytes();
        int wordMatchIndex = 0;
        for (int i = 0, symbIndex = 0, lineIndex = 0; i < textChars.length; i++) {
            if (textChars[i] == '\n') {
                lineIndex++;
                symbIndex = 0;
            } else
                symbIndex++;
            if (textChars[i] == wordChars[wordMatchIndex]) {
                wordMatchIndex++;
                if (wordMatchIndex == wordChars.length)
                    return new Vector3i(symbIndex - wordChars.length + 1, lineIndex + 1, i - wordChars.length + 2);
            } else
                wordMatchIndex = 0;
        }
        return new Vector3i(-1, -1, -1);
    }

    public static Vector3i findWord(File file, String word) throws FileNotFoundException, IOException {
        if (file.isDirectory())
            return findWord(file, word, new StringBuilder());
        Vector3i wordIndex;
        FileInputStream fis = new FileInputStream(file);
        wordIndex = findWord(new String(fis.readAllBytes()), word);
        fis.close();
        return wordIndex;
    }

    public static Vector3i findWord(File dir, String word, StringBuilder matchFilePathBuff) throws FileNotFoundException, IOException {
        String[] dirContentNames = dir.list();
        for (String fileName : dirContentNames) {
            Vector3i wordIndex;
            File currFile = new File(dir.getAbsolutePath() + "\\" + fileName);
            if (currFile.isDirectory()) {
                wordIndex = findWord(currFile, word, matchFilePathBuff);
                if (wordIndex.x != -1)
                    return wordIndex;
            }
            wordIndex = findWord(currFile, word);
            if (wordIndex.x != -1) {
                matchFilePathBuff.append(currFile.getAbsolutePath());
                return wordIndex;
            }
        }
        return new Vector3i(-1, -1, -1);
    }
}