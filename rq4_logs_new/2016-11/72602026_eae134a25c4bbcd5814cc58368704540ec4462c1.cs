using UnityEngine;
using System.Collections;

public class Board : MonoBehaviour {

    private Piece[,] arrBoard;

    // Use this for initialization
    void Start() {
        arrBoard = new Piece[8, 8];
        SetBoard();
    }

    // Update is called once per frame
    void Update() {

    }

    //public Piece SelectPiece(Position pposPosition)
    //{
    //    if(arrBoard[pposPosition.getX(), pposPosition.getY()] != null)
    //    {
    //        arrBoard[pposPosition.getX(), pposPosition.getY()].SetSelected(true);
    //        PrintBoard();
    //        //return arrBoard[pintPosition.getX(), pintPosition.getY()];
    //    }

    //    return null;
    //}

    //Moves a given piece from one space to another if the move is valid. Returns true if a move was made.
    public bool SetPiece(Position pposPosition, Position pposMoveto) {
        if (arrBoard[pposPosition.getX(), pposPosition.getY()].ValidMove(arrBoard, pposMoveto)) {
            arrBoard[pposMoveto.getX(), pposMoveto.getY()] = null;
            arrBoard[pposPosition.getX(), pposPosition.getY()].SetPosition(pposMoveto);
            arrBoard[pposMoveto.getX(), pposMoveto.getY()] = arrBoard[pposPosition.getX(), pposPosition.getY()];
            arrBoard[pposPosition.getX(), pposPosition.getY()] = null;
            return true;
        }
        return false;
    }

    //Sets the initial state of the board.
    private void SetBoard() {
        //inital white piece positions
        Position posWhitePawn1 = new Position(0, 6);
        Position posWhitePawn2 = new Position(1, 6);
        Position posWhitePawn3 = new Position(2, 6);
        Position posWhitePawn4 = new Position(3, 6);
        Position posWhitePawn5 = new Position(4, 6);
        Position posWhitePawn6 = new Position(5, 6);
        Position posWhitePawn7 = new Position(6, 6);
        Position posWhitePawn8 = new Position(7, 6);

        //inital black piece positions
        Position posBlackPawn1 = new Position(0, 1);
        Position posBlackPawn2 = new Position(1, 1);
        Position posBlackPawn3 = new Position(2, 1);
        Position posBlackPawn4 = new Position(3, 1);
        Position posBlackPawn5 = new Position(4, 1);
        Position posBlackPawn6 = new Position(5, 1);
        Position posBlackPawn7 = new Position(6, 1);
        Position posBlackPawn8 = new Position(7, 1);

        //white pieces
        Pawn pwnWhite1 = new Pawn(true, posWhitePawn1);
        Pawn pwnWhite2 = new Pawn(true, posWhitePawn2);
        Pawn pwnWhite3 = new Pawn(true, posWhitePawn3);
        Pawn pwnWhite4 = new Pawn(true, posWhitePawn4);
        Pawn pwnWhite5 = new Pawn(true, posWhitePawn5);
        Pawn pwnWhite6 = new Pawn(true, posWhitePawn6);
        Pawn pwnWhite7 = new Pawn(true, posWhitePawn7);
        Pawn pwnWhite8 = new Pawn(true, posWhitePawn8);

        //black pieces
        Pawn pwnBlack1 = new Pawn(false, posBlackPawn1);
        Pawn pwnBlack2 = new Pawn(false, posBlackPawn2);
        Pawn pwnBlack3 = new Pawn(false, posBlackPawn3);
        Pawn pwnBlack4 = new Pawn(false, posBlackPawn4);
        Pawn pwnBlack5 = new Pawn(false, posBlackPawn5);
        Pawn pwnBlack6 = new Pawn(false, posBlackPawn6);
        Pawn pwnBlack7 = new Pawn(false, posBlackPawn7);
        Pawn pwnBlack8 = new Pawn(false, posBlackPawn8);


        //set white pieces on board
        arrBoard[posWhitePawn1.getX(), posWhitePawn1.getY()] = pwnWhite1;
        arrBoard[posWhitePawn2.getX(), posWhitePawn2.getY()] = pwnWhite2;
        arrBoard[posWhitePawn3.getX(), posWhitePawn3.getY()] = pwnWhite3;
        arrBoard[posWhitePawn4.getX(), posWhitePawn4.getY()] = pwnWhite4;
        arrBoard[posWhitePawn5.getX(), posWhitePawn5.getY()] = pwnWhite5;
        arrBoard[posWhitePawn6.getX(), posWhitePawn6.getY()] = pwnWhite6;
        arrBoard[posWhitePawn7.getX(), posWhitePawn7.getY()] = pwnWhite7;
        arrBoard[posWhitePawn8.getX(), posWhitePawn8.getY()] = pwnWhite8;

        //set black pieces on board
        arrBoard[posBlackPawn1.getX(), posBlackPawn1.getY()] = pwnBlack1;
        arrBoard[posBlackPawn2.getX(), posBlackPawn2.getY()] = pwnBlack2;
        arrBoard[posBlackPawn3.getX(), posBlackPawn3.getY()] = pwnBlack3;
        arrBoard[posBlackPawn4.getX(), posBlackPawn4.getY()] = pwnBlack4;
        arrBoard[posBlackPawn5.getX(), posBlackPawn5.getY()] = pwnBlack5;
        arrBoard[posBlackPawn6.getX(), posBlackPawn6.getY()] = pwnBlack6;
        arrBoard[posBlackPawn7.getX(), posBlackPawn7.getY()] = pwnBlack7;
        arrBoard[posBlackPawn8.getX(), posBlackPawn8.getY()] = pwnBlack8;
    }
}