public class EvilOverlord {




    public void updateBoard(CellGrid cellGrid) {
        // loop over every cell

        for(int cellRow = 0; cellRow < cellGrid.getNumberOfRows(); cellRow++){
            for(int cellCol = 0; cellCol < cellGrid.getNumberOfColumns(); cellCol++){

                boolean stateOfNeighbourAbove = getStateOfNeighbourAbove(cellGrid, cellRow, cellCol);
                boolean stateOfNeighbourToTopRightCorner = getStateOfNeighbourToTheTopRightCorner(cellGrid, cellRow, cellCol);
                boolean stateOfNeighbourToTheRight = getStateOfNeighbourToTheRight(cellGrid, cellRow, cellCol);
                boolean stateOfNeighbourToTheBottomRightCorner = getStateOfNeighbourToTheBottomRightCorner(cellGrid, cellRow, cellCol);
                boolean stateOfNeighbourBelow = getStateOfNeighbourBelow(cellGrid, cellRow, cellCol);
                boolean stateOfNeighbourToTheBottomLeftCorner = getStateOfNeighbourToTheBottomLeftCorner(cellGrid, cellRow, cellCol);
                boolean stateOfNeighbourToTheLeft = getStateOfNeighbourToTheLeft(cellGrid, cellRow, cellCol);
                boolean stateOfNeighbourToTheTopLeftCorner = getStateOfNeighbourToTheTopLeftCorner(cellGrid, cellRow, cellCol);

            }
        }
    }

    private boolean getStateOfNeighbourAbove(CellGrid cellGrid, int cellRow, int cellCol){
        if(cellRow > 0){
           return cellGrid.getCellIsAlive(cellRow-1, cellCol);
        }
        return cellGrid.getCellIsAlive(cellGrid.getNumberOfRows(), cellCol);
    }

    private boolean getStateOfNeighbourToTheTopRightCorner(CellGrid cellGrid, int cellRow, int cellCol){
        if((cellRow > 0) && (cellCol < cellGrid.getNumberOfColumns())){
            return cellGrid.getCellIsAlive(cellRow-1, cellCol +1);
        }
//        top LH corner and top edge
        if((cellRow == 0) && (cellCol < cellGrid.getNumberOfColumns())){
            return cellGrid.getCellIsAlive(cellGrid.getNumberOfRows(), cellCol+1);
        }
//        top RH corner
        if((cellRow == 0) && (cellCol == cellGrid.getNumberOfColumns()){
            return cellGrid.getCellIsAlive(cellGrid.getNumberOfRows(), 0);
        }
//        RH edge and bottom RH corner
        if((cellRow > 0) && (cellCol == cellGrid.getNumberOfColumns()){
            return cellGrid.getCellIsAlive(cellRow -1, 0);
        }
    }

    private boolean getStateOfNeighbourToTheRight(CellGrid cellGrid, int cellRow, int cellCol){

        if (cellCol < cellGrid.getNumberOfColumns()){
            return cellGrid.getCellIsAlive(cellRow, cellCol+1);
        }

        return cellGrid.getCellIsAlive(cellRow,0);

    }

    private boolean getStateOfNeighbourToTheBottomRightCorner(CellGrid cellGrid, int cellRow, int cellCol){
        if((cellRow < cellGrid.getNumberOfRows()) && (cellCol < cellGrid.getNumberOfColumns())){
            return cellGrid.getCellIsAlive(cellRow +1, cellCol +1);
        }
//        top RH corner and RH edge
        if((cellRow < cellGrid.getNumberOfRows()) && (cellCol == cellGrid.getNumberOfColumns()){
            return cellGrid.getCellIsAlive(cellRow + 1, 0);
        }
//        bottom RH corner
        if((cellRow == cellGrid.getNumberOfRows()) && (cellCol == cellGrid.getNumberOfColumns())){
            return cellGrid.getCellIsAlive(0, 0);
        }
//        bottom LH corner and bottom edge
        if((cellRow == cellGrid.getNumberOfRows()) && (cellCol < cellGrid.getNumberOfColumns())){
            return cellGrid.getCellIsAlive(0, cellCol + 1);
        }
    }

    private boolean getStateOfNeighbourBelow(CellGrid cellGrid, int cellRow, int cellCol){
        if(cellRow < cellGrid.getNumberOfColumns()){
            return cellGrid.getCellIsAlive(cellRow+1, cellCol);
        }
            return cellGrid.getCellIsAlive(0, cellCol);
    }

    private boolean getStateOfNeighbourToTheBottomLeftCorner(CellGrid cellGrid, int cellRow, int cellCol){
        return 
    }

    private boolean getStateOfNeighbourToTheLeft(CellGrid cellGrid, int cellRow, int cellCol){
        if (cellCol > 0){
            return cellGrid.getCellIsAlive(cellRow, cellCol-1);
        }
            return cellGrid.getCellIsAlive(cellRow, cellGrid.getNumberOfColumns());
    }

    private boolean getStateOfNeighbourToTheTopLeftCorner(CellGrid cellGrid, int cellRow, int cellCol) {
        if ((cellRow > 0) && (cellCol > 0)) {
            return cellGrid.getCellIsAlive(cellRow - 1, cellCol - 1);
        }
        //        top RH corner and top edge
        if ((cellRow == 0) && (cellCol > 0)) {
            return cellGrid.getCellIsAlive(cellGrid.getNumberOfRows(), cellCol - 1);
        }
        //        top LH corner
        if ((cellRow == 0) && (cellCol == 0)) {
            return cellGrid.getCellIsAlive(cellGrid.getNumberOfRows(), cellGrid.getNumberOfColumns());
        }
        //        LH edge and bottom LH corner
        if ((cellRow > 0) && (cellCol == 0)) {
            return cellGrid.getCellIsAlive(cellRow - 1, cellGrid.getNumberOfColumns());
        }
    }


}