package Homework_6;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {
        //Создать 2 текстовых файла, примерно по 50-100 символов в каждом (особого значения не имеет);
        StringBuilder stringBuilder = new StringBuilder();

        try {
            //Написать программу, «склеивающую» эти файлы, то есть вначале идет текст из первого файла, потом текст из второго.
            //то есть вначале идет текст из первого файла
            FileInputStream fis = new FileInputStream("src\\Homework_6\\HomeworkBasic.txt");
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            Reader r = new BufferedReader(isr);
            int c = 0;
            while ((c = r.read()) != -1){
                stringBuilder.append((char) c);
            }
            stringBuilder.append("\n");
            r.close();

            //потом текст из второго.
            fis = new FileInputStream("src\\Homework_6\\HomeworkAdvanced.txt");
            isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            r = new BufferedReader(isr);
            c = 0;
            while ((c = r.read()) != -1){
                stringBuilder.append((char) c);
            }
            r.close();

            //«склеивающую» эти файлы
            FileOutputStream fos = new FileOutputStream("src\\Homework_6\\Homework_full.txt");
            PrintStream ps = new PrintStream(fos, true, StandardCharsets.UTF_8);
            ps.print(stringBuilder);
            ps.close();

            //* Написать программу, которая проверяет присутствует ли указанное пользователем слово в файле.
            if (searchForWord("src\\Homework_6\\Homework_full.txt", "программу")) System.out.println("Word was found!");
            else System.out.println("Word wasn't found");
            if (searchForWord("src\\Homework_6\\Homework_full.txt", "файл")) System.out.println("Word was found!");
            else System.out.println("Word wasn't found");

            //** Написать метод, проверяющий, есть ли указанное слово в папке, внутри есть файлы, в которых может содержатся это слово
            searchWordInFiles("src\\Homework_6\\", "программу");
            searchWordInFiles("src\\Homework_6\\", "файл");
            searchWordInFiles("src\\Homework_6\\", "проверяющий");

        }  catch (IOException e) {
            e.printStackTrace();
        }
    }

    //* Написать программу, которая проверяет присутствует ли указанное пользователем слово в файле.
    public static boolean searchForWord(String path, String wordToSearch){
        try {
            FileInputStream fis = new FileInputStream(path);
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            BufferedReader r2 = new BufferedReader(isr);

            String[] wordsFromFile = null;
            String currentLine;

            while ((currentLine = r2.readLine()) != null) {
                wordsFromFile = currentLine.split(" ");
                for (String s : wordsFromFile) {
                    if (s.replaceAll("\\p{Punct}", "").toLowerCase().equals(wordToSearch)) {
                        r2.close();
                        return true;
                    }
                }
            }
            r2.close();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        return false;
    }

    //** Написать метод, проверяющий, есть ли указанное слово в папке, внутри есть файлы, в которых может содержатся это слово
    public static boolean searchWordInFiles(String startingFolder, String wordToSearch){
        boolean wordFound = false; //может в будущем понадобится. заглушка
        ArrayList<String> pathnames = new ArrayList<String>(0);
        File folder = new File(startingFolder);

        if(folder.listFiles() != null) {
            for (File file : folder.listFiles()) {
                if (file.isFile()) //создал папку и после первой проверки сыпался access denied. сделаем вид что глубина поиска = 0 :)
                    pathnames.add(file.getAbsolutePath());
            }
        }

        if (pathnames.size() == 0) return wordFound;

        System.out.println();
        for (int i = 0; i < pathnames.size(); i++) {
                wordFound(searchForWord(pathnames.get(i), wordToSearch), pathnames.get(i), wordToSearch);
                wordFound = true;
        }

        return wordFound;
    }

    //хотелось чтоб показывало в каком именно файле надено, а не "в списке непонятно каких файлов найдено/нет"
    public static void wordFound(boolean b, String filepath, String wordToFind){
        System.out.println("\"" + wordToFind + "\"" +
                ((b) ? " found in " + filepath : " wasn't found in " + filepath));
    }
}