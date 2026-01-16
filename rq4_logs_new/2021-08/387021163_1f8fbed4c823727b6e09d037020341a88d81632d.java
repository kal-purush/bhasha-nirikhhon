package Lessons4;

import org.w3c.dom.ls.LSOutput;

import java.awt.*;
import java.util.Random;
import java.util.Scanner;
import java.util.Vector;

public abstract class GameApp {

        public static Random random = new Random();
        public static Scanner scanner = new Scanner(System.in);

        public static char[][] map;
        public static char[][] invisibleMap;
        public static int mapWidth;
        public static int mapHeight;
        public static int mapSizeMin = 3;
        public static int mapSizeMax = 5;
        public static int mapCountLevel = 0;

        public static char player = '@';
        public static int playerHealth = 100;
        public static int playerPowerPoint = 15;
        public static int playerPosX;
        public static int playerPosY;
        public static int playerCountStep = 0;
        public static boolean setRandomStartPosition = false;

        public static final int moveUp = 8;
        public static final int moveDown = 2;
        public static final int moveLeft = 4;
        public static final int moveRight = 6;
        public static final int moveLeftUpRightDownDiag = 7;
        public static final int moveRightDownLeftUpDiag = 3;
        public static final int moveLeftDownRightUpDiag = 1;
        public static final int moveRightUpLeftDownDiag = 9;

        public static char enemy = 'E';
        public static int enemyHealth;
        public static int enemyPower;
        public static int enemyValueMin = 10;
        public static int enemyValueMax = 30;

        public static char readyCell = '*';
        public static char emptyCell = '_';
        public static char healthCell = 'X';
        public static char exitCell = '&';


        public static void main(String[] args) {

            while (isPlayerAlive()) {
                ++mapCountLevel;
                System.out.println("===== Game Start! Current LEVEL " + mapCountLevel);
                gameCycle();
            }
            System.out.println("===== Game Over! Count player steps: " + playerCountStep + ". Count ready level " + mapCountLevel);
        }

        public static void gameCycle() {
            createMap();
            createPlayer(setRandomStartPosition);
            createEnemies();
            createExit();
            createHealth();
            exitFound();


            while (true) {
                printMap();
                System.out.println("[Player info] > Health: " + playerHealth + ". Count steps: " + playerCountStep);
                changePositionPlayer();

                if (!isPlayerAlive()) {
                    System.out.println("Player is Dead!");
                    break;
                }

                if (isFullMap()) {
                    System.out.println("Map is full");
                    break;
                }
                if (exitFound()) {
                    System.out.println("You found exit");
                    break;
                }
            }
        }

        public static void createMap() {
            mapHeight = randomRange(mapSizeMin, mapSizeMax);
            mapWidth = randomRange(mapSizeMin, mapSizeMax);
            map = new char[mapHeight][mapWidth];
            invisibleMap = new char[mapHeight][mapWidth];

            for (int y = 0; y < mapHeight; y++) {
                for (int x = 0; x < mapWidth; x++) {
                    map[y][x] = emptyCell;
                }
            }

            System.out.println("Create map > size " + mapWidth + "x" + mapHeight);
        }

        public static void printMap() {
            System.out.println("===== CURRENT MAP =====");
            for (int y = 0; y < mapHeight; y++) {
                for (int x = 0; x < mapWidth; x++) {
                    System.out.print(map[y][x] + "|");
                }
                System.out.println();
            }
            System.out.println("=======================");
        }

        public static void createPlayer(boolean randomPositionPlayer) {
            if (randomPositionPlayer) {
                playerPosX = randomRange(0, mapWidth - 1);
                playerPosY = randomRange(0, mapHeight - 1);
            } else {
                playerPosX = mapWidth / 2;
                playerPosY = mapHeight / 2;
            }
            map[playerPosY][playerPosX] = player;
        }

        public static void createEnemies() {
            enemyHealth = randomRange(enemyValueMin, enemyValueMax);
            enemyPower = randomRange(enemyValueMin, enemyValueMax);

            int posX;
            int posY;

            int countEnemies = (mapWidth + mapHeight) / 4;

            for (int i = 0; i < countEnemies; i++) {

                do {
                    posX = random.nextInt(mapWidth);
                    posY = random.nextInt(mapHeight);
                } while (posX == playerPosX && posY == playerPosY);

                invisibleMap[posY][posX] = enemy;
            }
            System.out.println("Enemy count: " + countEnemies + ". Enemy power: " + enemyPower + ". Enemy health: " + enemyHealth);

        }
        public static void createExit(){
            int posX;
            int posY;

            do {
                posX = random.nextInt(mapWidth);
                posY = random.nextInt(mapHeight);
            } while ((posX == playerPosX && posY == playerPosY)||(invisibleMap[posY][posX] == enemy));
            invisibleMap[posY][posX] = exitCell;
        }
    public static void createHealth(){
        int posX;
        int posY;

        do {
            posX = random.nextInt(mapWidth);
            posY = random.nextInt(mapHeight);
        } while ((posX == playerPosX && posY == playerPosY)||(invisibleMap[posY][posX] == enemy)||(invisibleMap[posY][posX] == exitCell));
        invisibleMap[posY][posX] = healthCell;

    }

        public static void changePositionPlayer() {
            int currentX = playerPosX;
            int currentY = playerPosY;

            int playerMove;

            do {
                System.out.print("Enter your move (UP=" + moveUp +
                        " DOWN=" + moveDown +
                        " LEFT=" + moveLeft +
                        " RIGHT=" + moveRight +
                        " LDRU=" + moveLeftDownRightUpDiag +
                        " RULD=" + moveRightUpLeftDownDiag +
                        " LURD=" + moveLeftUpRightDownDiag +
                        " RDLU=" + moveRightDownLeftUpDiag + ") >>> "
                );
                playerMove = scanner.nextInt();

                switch (playerMove) {
                    case moveUp:
                        playerPosY -= 1;
                        break;
                    case moveDown:
                        playerPosY += 1;
                        break;
                    case moveLeft:
                        playerPosX -= 1;
                        break;
                    case moveRight:
                        playerPosX += 1;
                        break;
                    case moveLeftDownRightUpDiag:
                        playerPosX -= 1;
                        playerPosY += 1;
                        break;
                    case moveRightUpLeftDownDiag:
                        playerPosX += 1;
                        playerPosY -= 1;
                        break;
                    case moveRightDownLeftUpDiag:
                        playerPosX += 1;
                        playerPosY += 1;
                        break;
                    case moveLeftUpRightDownDiag:
                        playerPosX -= 1;
                        playerPosY -= 1;
                        break;
                }
            } while (!isValidNextMove(currentY, currentX, playerPosY, playerPosX));

            playerNextMoveAction(currentY, currentX, playerPosY, playerPosX);
            ++playerCountStep;
        }

        public static boolean isValidNextMove(int currentY, int currentX, int nextY, int nextX) {
            if (nextY >= 0 && nextY < mapHeight && nextX >= 0 && nextX < mapWidth) {
                System.out.println("Player move to [" + (nextX + 1) + ":" + (nextY + 1) + "]  success");
                return true;
            } else {
                playerPosX = currentX;
                playerPosY = currentY;
                System.out.println("Invalid move. Please try again");
                return false;
            }
        }

        public static void playerNextMoveAction(int currentY, int currentX, int nextY, int nextX) {
            if (invisibleMap[nextY][nextX] == enemy) {
                playerHealth -= enemyPower;
                System.out.println("Alert! Enemy give damage " + enemyPower + ". Player health now is " + playerHealth);
                enemyHealth -= playerPowerPoint;
            }
            if (invisibleMap[nextY][nextX] == exitCell){
                map[nextX][nextY] = exitCell;
            } else
                map[playerPosY][playerPosX] = player;


            invisibleMap[playerPosY][playerPosX] = readyCell;
            map[currentY][currentX] = readyCell;


        }

        public static int randomRange(int min, int max) {
            int diff = max - min;
            int value = random.nextInt(diff + 1);
            return min + value;
        }

        public static boolean isFullMap() {
            for (int y = 0; y < mapHeight; y++) {
                for (int x = 0; x < mapWidth; x++) {
                    if (map[y][x] == emptyCell) return false;
                }
            }
            return true;
        }

        public static boolean isPlayerAlive() {
            return playerHealth > 0;
        }
    public static boolean exitFound() {
        for (int y = 0; y < mapHeight; y++) {
            for (int x = 0; x < mapWidth; x++) {
                if (map[y][x] == exitCell) return true;
            }
        }
        return false;
        
    }



        }

