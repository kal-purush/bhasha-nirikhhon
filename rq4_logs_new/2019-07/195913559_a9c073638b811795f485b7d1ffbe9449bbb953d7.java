public class CellAndGridFormatter {


    public String formatCellGrid(String[][] cellGrid){
        String formattedGrid = "";
        for(int row = 0; row <cellGrid.length; row++){
            formattedGrid = formattedGrid.concat("\n");
            for(int column = 0; column < cellGrid.length; column++){
                formattedGrid = formattedGrid.concat(cellGrid[row][column] + " ");
            }
        }
        return formattedGrid;
    }

//    private String formatCell(){
//        if(!this.isAlive){
//            cell = "x";
//        }
//        else {
//            cell = "o";
//        }
//        return cell;
//    }

}