import java.util.ArrayList;

public class GameManager {
   private GameRules gameRules = new GameRules();
   private CellGrid cellGrid;


    public CellGrid initiateGame(){

        PrintToConsole.enterDimensionsInstruction();

        int[] value = {};
        try {
            value = UserInputManager.getCellGridDimensions();
        }
        catch (IncorrectInputException message){
            System.err.println(message.getMessage());
        }

        PrintToConsole.enterLiveCellCoordinates();

        cellGrid = new CellGrid(value[0], value[1]);

        try {
            ArrayList<Coordinates> coords = UserInputManager.getCoordinatesOfLiveCells();
            cellGrid.setCellState(coords);
        }
        catch (IncorrectInputException message){
            System.err.println(message.getMessage());
        }
        return cellGrid;
    }

    public void runGame() throws InterruptedException{

        for (int turns = 0; turns < 20; turns++) {

            System.out.println("\n");
            String [][] printableCellGrid = CellGridTranslator.getCellGridAsStringArray(cellGrid);
            cellGrid = updateCells(cellGrid);
            PrintToConsole.printCellGrid(CellGridTranslator.formatStringGridAsSingleString(printableCellGrid));

            Thread.sleep(1000);

        }
    }

    private CellGrid updateCells(CellGrid cellGrid){
        ArrayList<Coordinates> nextGenCells = gameRules.decideCellFate(cellGrid);
        cellGrid = new CellGrid(cellGrid.getNumberOfRows(), cellGrid.getNumberOfColumns());
        cellGrid.setCellState(nextGenCells);
        return cellGrid;
    }
}