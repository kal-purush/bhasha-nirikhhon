package hw1_2_Level2;

public class App {
    public static void main(String[] args) {
        //массив игроков
        Player[] players = {
                new Cat("Bony", 120, 1),
                new Human("Syogun", 3000, 1),
                new Robot("R2D2", 100000, 0),
                new Cat("Bella", 300, 3),
                new Robot("b-corporation 2.33s", 23000, 23),
                new Human("Sergei", 10000, 2),
                new Robot("MS-h6", 4000, 8),
                new Cat("Benny", 58, 2),
                new Human("Sebastyan", 5000, 1)
        };

        //массив препятствий
        Barrier[] barriers = {
                new Wall(1),
                new Track(9000),
                new Wall(2),
                new Track(1000)
        };

        //для барьеров делаем цикл
        for (Barrier barrier : barriers) {
            //цикл для участников
            for (Player player : players) {
                barrier.overcome(player);
            }
        }
        System.out.println("И на финише мы наблюдаем:");
        //проходим циклом через игроков и проверяем кто в игре
        for (Player player : players) {
            if (player.isPlay()) {
                System.out.println(player);
            }
        }
    }
}