import org.junit.Test;

import static org.junit.Assert.*;

public class CellGridTranslatorTest {

    @Test
    public void whenGivenA2dArrayOfCellsReturnsA2dArrayOfStrings(){
        CellGrid cellGrid = new CellGrid(3,3);

        String[][] expectedOutput = {{"x", "x", "x"}, {"x", "x", "x"}, {"x", "x", "x"}};
        String[][] actualOutput = CellGridTranslator.getCellGridAsStringArray(cellGrid);

        System.out.println(cellGrid.getCellIsAlive(0,0));
        assertArrayEquals(expectedOutput, actualOutput);

    }

    @Test
    public void whenACellsIsAliveStringIsSetToAnO(){
        CellGrid cellGrid = new CellGrid(3,3);
        Cell cell = new Cell();
        

    }

    @Test
    public void whenACellIsDeadStringIsSetToAnX(){

    }

}