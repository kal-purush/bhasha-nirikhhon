package ru.geekbrains.java3.client.model;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileService {
    private static String FILE_ARCHIVE_TEMPLATE =
            "#archive.log";
    private static String archiveFileName;
    private static BufferedWriter archiveWriter;

    public void setArchiveFileName(String userName) {
        this.archiveFileName = userName+FILE_ARCHIVE_TEMPLATE;
    }

    // Сохранить сообщение в файл истории чата
    public static void putMessageToArchveFile(String message) throws IOException {
        //Открыть файл истории чата на запись
        if (archiveWriter==null) {
            archiveWriter =
                    new BufferedWriter(new FileWriter(archiveFileName,
                            true));
        }
         archiveWriter.write(message);
         archiveWriter.newLine();
    }

    // Получить историю сообщений из архивного файла
    public static List getArchiveFromFile(long numLastMessages)
            throws IOException {
        ArrayList messages = new ArrayList<String>();

        try (FileInputStream fis = new FileInputStream(archiveFileName)){
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(fis));
            String str;
            long i=numLastMessages;
            while ((str=br.readLine())!=null && i>0) {
                messages.add(str);
                i--;
            }
        }
        return messages;
    }

    // Закрыть поток для записи истории чата
    public void close() {
        if (archiveWriter==null)
            return;
        try {
//            archiveWriter.flush();
            archiveWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}