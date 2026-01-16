import java.util.Random;
import java.util.Scanner;

public class HomeWorkApp4 {

    public static class Lesson04 {

        public static char[][] map;
        public static int mapSizeX;
        public static int mapSizeY;

        public static char human = 'X';
        public static char ai = 'O';
        public static char empty_field = '_';

        public static Scanner scanner = new Scanner(System.in);
        public static Random random = new Random();

        public static void createMap() {
            mapSizeX = 3;
            mapSizeY = 3;
            map = new char[mapSizeY][mapSizeX];

            for (int y = 0; y < mapSizeY; y++) {
                for (int x = 0; x < mapSizeX; x++) {
                    map[y][x] = empty_field;
                }
            }
        }

        public static void showMap() {
            for (int y = 0; y < mapSizeY; y++) {
                for (int x = 0; x < mapSizeX; x++) {
                    System.out.print(map[y][x] + "|");
                }
                System.out.println();
            }
            System.out.println("----------");
        }

        public static void humanTurn() {
            int x;
            int y;

            do {
                System.out.println("Enter your turn coordinates from 1 before " + mapSizeX + ":");
                y = scanner.nextInt() - 1;
                x = scanner.nextInt() - 1;
            } while (!isValidCell(y, x) || !isEmptyCell(y, x));
            map[y][x] = human;
        }

        public static void aiTurn() {
            int x;
            int y;

            do {
                y = random.nextInt(mapSizeX); //[0;mapSize)
                x = random.nextInt(mapSizeY);
            } while (!isEmptyCell(y, x));
            System.out.println("AI turn is [" + (y + 1) + ":" + (x + 1) + "]");
            map[y][x] = ai;
        }

        public static boolean isValidCell(int y, int x) {
            return y >= 0 && y < mapSizeY && x >= 0 && x < mapSizeX;
        }

        public static boolean isEmptyCell(int y, int x) {
            return map[y][x] == empty_field;
        }

        public static boolean isDraw() {
            for (int y = 0; y < mapSizeY; y++) {
                for (int x = 0; x < mapSizeX; x++) {
                    if (map[y][x] == empty_field) {
                        return false;
                    }
                }
            }
            return true;
        }

        public static boolean isWin(char player) {
            if (map[0][0] == player && map[0][1] == player && map[0][2] == player) return true;
            if (map[1][0] == player && map[1][1] == player && map[1][2] == player) return true;
            if (map[2][0] == player && map[2][1] == player && map[2][2] == player) return true;

            if (map[0][0] == player && map[1][0] == player && map[2][0] == player) return true;
            if (map[0][1] == player && map[1][1] == player && map[2][1] == player) return true;
            if (map[0][2] == player && map[1][2] == player && map[2][2] == player) return true;

            if (map[0][0] == player && map[1][1] == player && map[2][2] == player) return true;
            if (map[0][2] == player && map[1][1] == player && map[2][0] == player) return true;

            return false;
        }

//    public static void createMap(int sizeX, int sizeY) {
//        mapSizeX = sizeX;
//        mapSizeY = sizeY;
//        map = new char[mapSizeY][mapSizeX];
//    }

        public static void main(String[] args) {
            createMap();
            showMap();

            while (true) {
                humanTurn();
                showMap();
                if (isWin(human)) {
                    System.out.println("Human win!!!");
                    break;
                }

                if (isDraw()) {
                    System.out.println("Is Draw!!!");
                    break;
                }

                aiTurn();
                showMap();

                if (isWin(ai)) {
                    System.out.println("AI win!!!");
                    break;
                }

                if (isDraw()) {
                    System.out.println("Is Draw!!!");
                    break;
                }
            }
            System.out.println("Game over!");
        }
    }
}