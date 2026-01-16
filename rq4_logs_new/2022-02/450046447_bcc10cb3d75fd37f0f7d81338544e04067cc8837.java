package client;


import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SaveStoryService {

    void createFileOfHistory(String nickname) {
        String dirName = "history";
        File dir = new File("client/src/main/resources", dirName);
        if (!dir.exists()) {
            dir.mkdir();
        }
        String fileName = nickname + ".txt";
        File file = new File(dir, fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    void saveMsg(String msg, String nickname) {
        String fileName = "client/src/main/resources/history/" + nickname + ".txt";


        try (
                FileWriter fileWriter = new FileWriter(fileName)) {
            fileWriter.write(msg + "\t");
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    String getHistoryMsg(String nickname) {
        String fileName = "client/src/main/resources/history/" + nickname + ".txt";
        StringBuilder result = new StringBuilder();
        try (
                FileReader fileReader = new FileReader(fileName)) {
            BufferedReader reader = new BufferedReader(fileReader);
            StringBuilder story = new StringBuilder();
            while (reader.ready()) {
                story.append(reader.readLine()).append("\t");
            }
            String str = story.toString();
            String[] arrStr = str.split("\t");
            List<String> list = new ArrayList<>(Arrays.asList(arrStr));

            if (list.size() >= 100) {
                for (int i = list.size() - 100; i < list.size(); i++) {
                    result.append(list.get(i)).append("\n");
                }
            } else {
                for (int i = 0; i < list.size(); i++) {
                    result.append(list.get(i)).append("\n");
                }
            }
//            String[] arrStr = str.split("\t");
//            List<String> list = new ArrayList<>(Arrays.asList(arrStr));
//            if (list.size() >= 100) {
//                for (int i = list.size() - 100; i < list.size(); i++) {
//                   textArea.appendText(list.get(i) + "\n");
//                }
//            } else {
//                for (String s : list) {
//                    textArea.appendText(s + "\n");
//                }
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result.toString();
    }
}