import java.awt.*;

/**
 * Created by Danyo on 2017-06-22.
 */
public class Cell {

    int BORDER_WIDTH = 1 ;
    int CELL_WIDTH = 16 ;
    int CELL_HEIGHT = 16 ;

    Color BORDER_COLOR_DARK     = new Color(8, 57, 90) ;
    Color BORDER_COLOR_LIGHT    = new Color(24, 74, 115) ;
    Color BORDER_COLOR_WALL    = new Color(148, 255, 239) ;

    Color CELL_COLOR_DEFAULT    = new Color(0, 41, 74) ;
    Color CELL_COLOR_BLUE       = new Color(49, 133, 181) ;

    Color cellColor ;
    Color borderTop ;
    Color borderLeft ;
    Color borderBottom ;
    Color borderRight ;

    int height ;
    int width ;
    int border ;

    public Cell() {
        height = CELL_HEIGHT ;
        width = CELL_WIDTH ;
        border = BORDER_WIDTH ;

        cellColor = CELL_COLOR_DEFAULT ;
        borderTop = BORDER_COLOR_DARK ;
        borderRight = BORDER_COLOR_DARK ;
        borderBottom = BORDER_COLOR_DARK ;
        borderLeft = BORDER_COLOR_DARK ;
    }

    public void borderSectionTop() {
        borderTop = BORDER_COLOR_LIGHT ;
    }

    public void borderSectionRight() {
        borderRight = BORDER_COLOR_LIGHT ;
    }

    public void borderSectionBottom() {
        borderBottom = BORDER_COLOR_LIGHT ;
    }

    public void borderSectionLeft() {
        borderLeft = BORDER_COLOR_LIGHT ;
    }

    public void paintBlue() {
        cellColor = CELL_COLOR_BLUE ;
    }

    public void wallTop() {
        borderTop = BORDER_COLOR_WALL ;
    }

    public void wallRight() {
        borderRight = BORDER_COLOR_WALL ;
    }

    public void wallBottom() {
        borderBottom = BORDER_COLOR_WALL ;
    }

    public void wallLeft() {
        borderLeft = BORDER_COLOR_WALL ;
    }

    public int getHeight() {
        return height ;
    }

    public int getWidth() {
        return width ;
    }

    public int getBorder() {
        return border ;
    }

    public Color getCellColor() {
        return cellColor ;
    }
}