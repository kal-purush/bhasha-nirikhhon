package com.company;

import java.util.Random;

public class Board {

    public static final int BOARD_SIZE = 10;
    public static final int SHIP_TYPES_COUNT = 4;

    private Field[][] fields = new Field[BOARD_SIZE][BOARD_SIZE];

    private int shipsCount;
    private int numberOfShipsByDeck[] = new int[SHIP_TYPES_COUNT];

    public Board() {
        for (int y = 0; y < BOARD_SIZE; y++) {
            for (int x = 0; x < BOARD_SIZE; x++) {
                fields[y][x] = new Field(x, y, State.EMPTY);
            }
        }
    }

    public void fillBoard() {

        Random random = new Random();
        for (int decks = 1; decks <= SHIP_TYPES_COUNT ; decks++) {
            for (int i = 0; i < getTotalCountOfShips(decks); i++) {

                boolean tryAgain;
                do {
                    int x = random.nextInt(BOARD_SIZE);
                    int y = random.nextInt(BOARD_SIZE);
                    WarShip.Orientation orientation
                            = random.nextBoolean() ? WarShip.Orientation.HORIZONTAL
                            : WarShip.Orientation.VERTICAL;

                    Ship ship = getShip(decks, orientation);

                    try {
                        addShip(x,y,ship);
                        tryAgain = false;
                    } catch (IllegalMoveException e) {
                        tryAgain = true;
                    }

                } while (tryAgain);

            }
        }
    }

    private Ship getShip(int decks, WarShip.Orientation orientation) {
        switch (decks) {
            case 1:
                return new Submarine(orientation);
            case 2:
                return new Destroyer(orientation);
            case 3:
                return new Cruiser(orientation);
            case 4:
                return new BattleShip(orientation);
            default:
                throw new IllegalArgumentException(
                        String.format("Unknown ship with %d decks", decks));
        }
    }

    private static void printLetters() {
        System.out.print(" ");
        for (int i = 0; i < BOARD_SIZE; i++) {
            System.out.print((char) ('A' + i));
        }
        System.out.print('\n');
    }

    public void printBoard() {
        printLetters();
        for (int i = 0; i < BOARD_SIZE; i++) {
            System.out.print(i);
            for (int j = 0; j < BOARD_SIZE; j++) {
                char shipValue = fields[i][j].stateToChar();
                System.out.print(shipValue);
            }
            System.out.print('\n');
        }
    }

    private static State getRandomShip(double random) {
        if (random < 0.3) {
            return State.HIT;
        } else if (random > 0.8) {
            return State.EMPTY;
        } else {
            return State.MISS;
        }
    }

    public void addShip(int x, int y, Ship ship) throws IllegalMoveException {

        int count = ship.getDecksCount();
        if(numberOfShipsByDeck[count - 1]
                == getTotalCountOfShips(count)){
            throw new IllegalMoveException("You have all submarines set!");
        }

        Field[] field = new Field[count];
        int xToSet = x, yToSet = y;
        for (int i = 0; i < count; i++) {
            if(ship.getOrientation() == WarShip.Orientation.HORIZONTAL){
                xToSet = x + i;
            } else {
                yToSet = y + i;
            }
            if (isOutside(xToSet, yToSet)) {
                throw new IllegalMoveException("Ship set outside board!");
            }

            field[i] = fields[yToSet][xToSet];
            if(isFieldOcupied(field[i])) {
                throw new IllegalMoveException("Field is occupied!");
            }
        }

        for (int i = 0; i < count; i++) {
            ship.setOnField(field[i], i);
        }
        
        shipsCount++;
        numberOfShipsByDeck[count - 1]++;
    }

    private boolean isFieldOcupied(Field field) {
        for (int y = field.getY() - 1 ; y < field.getY() + 1; y++) {
            for (int x = field.getX() - 1; x < field.getX() + 1 ; x++) {

                if(isOutside(x, y)){
                    continue;
                }

                if(fields[y][x].getState() != State.EMPTY){
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isOutside(int x, int y) {
        return x < 0 || x >= BOARD_SIZE || y < 0 || y >= BOARD_SIZE;
    }

    private int getTotalCountOfShips(int decksCount) {
        return SHIP_TYPES_COUNT - decksCount + 1;
    }

    public int getShipsCount() {
        return shipsCount;
    }

    public Field getField(int x, int y) {
        return fields[y][x];
    }

    public void shoot(int x, int y) throws IllegalMoveException {
        if(isOutside(x, y)){
            throw new IllegalMoveException("Don't shoot outside!");
        }

        Field field = getField(x, y);

        if(field.getState() == State.MISS ||
                field.getState() == State.HIT ||
                field.getState() == State.SUNK){
            throw new IllegalMoveException("You have already shoot here!");
        }

        if(field.getState() == State.EMPTY) {
            field.setState(State.MISS);
        } else if (field.getState() == State.SHIP) {
            field.setState(State.HIT);
            field.getShip().hit();
            if(field.getShip().isSunk()){
                shipsCount--;
            }
        }
    }
}