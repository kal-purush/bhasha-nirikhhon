package super_chess;

import piece.SuperPieceDecorator;

import java.awt.*;
import java.util.ArrayList;
import java.util.Map;

public class SuperGameRule {

    private SuperBoard superBoard;
    private Map<String, SuperPieceDecorator> superPiecesDict;

    public SuperGameRule(SuperBoard superBoard, Map<String, SuperPieceDecorator> superPiecesDict) {
        this.superBoard = superBoard;
        this.superPiecesDict = superPiecesDict;
    }

    public boolean isMoveValid(int oldX, int oldY, int newX, int newY) {
        if (!isCoordinateValid(oldX, oldY, newX , newY)) {
            System.out.println("Coordinate invalid");
            return false;
        }

        String pieceName = superBoard.getSuperPiece(oldX, oldY);
        String targetPieceName = superBoard.getSuperPiece(newX, newY);
        SuperPieceDecorator pieceToMove = superPiecesDict.get(pieceName);
        SuperPieceDecorator targetPiece = targetPieceName.equals("vacant") ? null : superPiecesDict.get(targetPieceName);
        String land = superBoard.getLandType(oldX, oldY);

        if (pieceToMove == null) {
            System.out.println("Piece not found");
            return false;
        }

        if (targetPiece != null && pieceToMove.hasSameColor(targetPiece)) {
            System.out.println("Invalid capture");
            return false;
        }

        if (!pieceToMove.validMove(oldX, oldY, newX , newY)) {
            System.out.println("Invalid Move");
            return false;
        }

        if (!pieceName.contains("knight") && !isPathClear(oldX, oldY, newX, newY)) {
            System.out.println("Path not clear");
            return false;
        }

        if (land.equals("Bridge") && !validFromBridgeAttack(oldX, oldY, newX, newY)){
            return false;
        }

        if (!land.equals("Bridge") && !validFromElsewhereAttack(oldX, oldY, newX, newY)){
            return false;
        }

        return true;
    }

    // If an attack/move is made from the bridge and the piece is...
    // a pawn: it can move to all positions
    // not a pawn: it can move to all positions except for the river
    public boolean validFromBridgeAttack(int oldX, int oldY, int newX, int newY){
        String pieceName = superBoard.getSuperPiece(oldX, oldY);
        if (!pieceName.equals("pawn")) {
            return !superBoard.getLandType(newX, newY).equals("river");
        }
        return true;
    }

    // if an attack is made from a land type that is not the bridge, it needs to...
    // Check: pieces besides the knight cannot move/attack over the bridge
    // Check: pieces cannot attack pawns "submerged" in the river or move into the river if it is not a pawn
    // Check: pieces cannot attack an opponent piece resting in its safe zone but can move into the opponent's safe zone
    public boolean validFromElsewhereAttack(int oldX, int oldY, int newX, int newY){
        String pieceName = superBoard.getSuperPiece(oldX, oldY);
        String targetLand = superBoard.getLandType(newX, newY);
        String targetPieceName = superBoard.getSuperPiece(newX, newY);

        // Check: pieces besides the knight cannot move/attack over the bridge
        if (!pieceName.equals("knight") && !isPathClearOfBridge(oldX, oldY, newX, newY)) {
            System.out.println("invalid attack/move over bridge");
            return false;
        }

        //Check: pieces cannot attack pawns "submerged" in the river or move into the river if it is not a pawn
        if (!pieceName.equals("pawn") && targetLand.equals("river")){
            System.out.println("invalid attack/move into river");
            return false;
        }

        // Check: pieces cannot attack an opponent piece resting in its safe zone but can move into the opponent's safe zone
        if (!targetPieceName.equals("vacant") && pieceName.charAt(0) != targetLand.charAt(0)){
            System.out.println("invalid attack on piece in safe zone");
            return false;
        }

        return true;
    }

    public boolean isCoordinateValid(int oldX, int oldY, int newX, int newY) {
        return newX >= 0 & newX < 13 & newY >= 0 & newY < 10 & (oldX != newX || oldY != newY);
    }

    // checks if path is clear of pieces
    public boolean isPathClear(int oldX, int oldY, int newX, int newY){
        ArrayList<Point> coordinates = pathCoordinates(oldX, oldY, newX, newY);
        for (Point point: coordinates) {
            if (!superBoard.isSuperPositionVacant(point.x, point.y)){
                return false;
            }
        }
        return true;
    }

    // checks if path is clear of bridges because pieces not on bridges can't attack over bridges
    public boolean isPathClearOfBridge(int oldX, int oldY, int newX, int newY){
        ArrayList<Point> coordinates = pathCoordinates(oldX, oldY, newX, newY);
        for (Point point: coordinates) {
            if (superBoard.getLandType(point.x, point.y).equals("bridge")){
                return false;
            }
        }
        return true;
    }

    // returns a list of the coordinates in the path between (oldX, oldY) and (newX, newY)
    public ArrayList<Point> pathCoordinates(int oldX, int oldY, int newX, int newY) {
        ArrayList<Point> coordinates = new ArrayList<>();

        if (oldY == newY) {
            // vertical north and south
            for (int i = Math.min(oldX, newX) + 1; i < Math.max(oldX, newX); i++) {
                coordinates.add(new Point(i, newY));
            }
        }

        else if (oldX == newX) {
            // horizontal east and west
            for (int i = Math.min(oldY, newY) + 1; i < Math.max(oldY, newY); i++) {
                coordinates.add(new Point(newX, i));
            }
        }

        else if ((oldX < newX & oldY < newY) || (oldX > newX & oldY > newY)) {
            // diagonal northwest or southeast
            for (int i = 1; i < Math.abs(newX - oldX); i++) {
                coordinates.add(new Point(Math.min(oldX, newX) + i, Math.min(oldY, newY) + i));
            }
        }

        else {
            // diagonal northeast or southwest
            for (int i = Math.abs(newX - oldX) - 1; i > 0; i--) {
                coordinates.add(new Point(Math.max(oldX, newX) - i, Math.min(oldY, newY) + i));
            }
        }

        return coordinates;
    }

}