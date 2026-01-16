package price.weston.amazeing;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by wprice on 4/23/17.
 */
public class BinaryTreeMazeGenerator implements MazeGenerator{

    @Override
    public Maze generate(CellBlock[][] grid) {

        for(int i = 0; i < grid.length; i++) {
            for(int j = 0; j < grid[i].length; j++) {
                CellBlock cb = grid[i][j];
                CellBlock connection = getAdjoiningBlock(grid, cb);
                if(connection != null) {
                    cb.addConnection(connection);
                }

            }
        }

        return new Maze(grid);
    }

    @Override
    public Maze generate(int rows, int columns) {
        return generate(MazeHelper.populateGrid(rows, columns));

    }


    private CellBlock getAdjoiningBlock(final CellBlock[][] grid, final CellBlock cellBlock) {
        List<CellBlock> cellBlocks = new ArrayList<>();

        if(!(cellBlock.getRow() + 1 > grid.length - 1)) {
            cellBlocks.add(grid[cellBlock.getRow() + 1][cellBlock.getColumn()]);
        }

        if(!(cellBlock.getColumn() + 1 > grid[cellBlock.getRow()].length - 1)) {
            cellBlocks.add(grid[cellBlock.getRow()][cellBlock.getColumn() + 1]);
        }

        return (!cellBlocks.isEmpty()) ? cellBlocks.get(new Random().nextInt(cellBlocks.size())) : null;
    }

}