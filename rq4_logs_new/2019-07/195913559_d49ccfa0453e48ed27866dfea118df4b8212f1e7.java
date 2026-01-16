import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class CellGridTest {

    @Test
    public void cellGridInitialisesWithSpecifiedNumberOfRowsAndColumnsOfCells(){
        CellGrid cellGrid = new CellGrid(6,6);

        ArrayList<Integer> expectedRowsAndColumns = new ArrayList<>();
        expectedRowsAndColumns.add(6);
        expectedRowsAndColumns.add(6);

        ArrayList<Integer> actualRowsAndColumns = new ArrayList<>();
        actualRowsAndColumns.add(cellGrid.getRows());
        actualRowsAndColumns.add(cellGrid.getColumns());

        assertEquals(expectedRowsAndColumns, actualRowsAndColumns);
    }

    @Test
    public void getCellIsDeadReturnsCurrentStateOfCellAtSpecifiedCoordinates(){
        CellGrid cellGrid = new CellGrid(6,6);
        boolean cellIsAlive = cellGrid.getDefaultCellStatus(2,2);

        assertFalse(cellIsAlive);
    }


}