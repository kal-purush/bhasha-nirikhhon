package pl.gacik.tictac;

import pl.gacik.coordinates.ICoordinates2D;

import java.util.Optional;

public class DiagonalBottomLeftToTopRightWinChecker implements WinChecker {

    private int requiredSeries;

    private IBoard board;

    public DiagonalBottomLeftToTopRightWinChecker(IBoard board) {
        this.board = board;
    }

    @Override
    public boolean victoryAchieved(ICoordinates2D lastMove) {
        Optional<Sign> lastSign = board.getSign(lastMove);
        if (!lastSign.isPresent()) {
            throw new IllegalArgumentException("Under Given Coordinates is no any sign");
        }
        int counter = 1;
        counter += seriesGoingTopRight(lastMove.getTopRight(), lastSign.get());
        counter += seriesGoingBottomLeft(lastMove.getBottomLeft(), lastSign.get());
        return (counter >= requiredSeries);
    }

    private int seriesGoingTopRight(ICoordinates2D coordinates, Sign sign) {
        if(!board.getSign(coordinates).isPresent()) return 0;
        if(board.getSign(coordinates).get() != sign) return 0;
        return 1 + seriesGoingTopRight(coordinates.getTopRight(), sign);
    }

    private int seriesGoingBottomLeft(ICoordinates2D coordinates, Sign sign) {
        if(!board.getSign(coordinates).isPresent()) return 0;
        if(board.getSign(coordinates).get() != sign) return 0;
        return 1 + seriesGoingBottomLeft(coordinates.getBottomLeft(), sign);
    }

    @Override
    public void setRequiredSeries(int requiredSeries) {
        this.requiredSeries = requiredSeries;
    }

}