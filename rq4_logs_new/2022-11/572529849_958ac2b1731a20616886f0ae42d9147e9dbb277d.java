// В консоли запросить имя пользователя. в зависимости от текущего времени, вывести приветствие
package java_projects;
import java.util.Scanner; 
import java.time.LocalDateTime;

public class task3 {
    public static void main(String[] args) {
        Scanner iScanner = new Scanner(System.in);
        System.out.printf("name: ");
        String name = iScanner.nextLine();
        int hour = LocalDateTime.now().getHour();
        if(hour > 4 && hour < 12){
            System.out.printf("Доброе утро, %s!\n", name);
        }
        else if(hour > 11 && hour < 18){
            System.out.printf("Добрый день, %s!\n", name);
        }
        else if(hour > 17 && hour < 23){
            System.out.printf("Добрый вечер, %s!\n", name);
        }
        else System.out.printf("Доброй ночи, %s!\n", name);
        iScanner.close();
    }
}