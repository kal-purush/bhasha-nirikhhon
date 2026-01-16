import java.io.*;
import java.util.Scanner;
import javax.swing.JOptionPane;

public class Main {
    static int count = 0;
    public static void main (String [ ] args) {
        String pathName = JOptionPane.showInputDialog("Укажите полный путь и название файла c расширением");
        String countWord = JOptionPane.showInputDialog("Укажите слово");
        File file = new File(pathName);
        try {
            Scanner scanner = new Scanner(file, "Windows-1251");
 
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                    if (line.contains(countWord)) {
                        count = count + 1;
                    }
            }
            JOptionPane.showMessageDialog(null, "Количество совпадений равно: " + count);
            scanner.close();
        } catch (IOException e) {
            JOptionPane.showMessageDialog(null, e.getMessage());
        }
    }
    }
