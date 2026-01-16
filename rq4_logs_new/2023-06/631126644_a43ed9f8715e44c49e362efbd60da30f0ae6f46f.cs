class PawnState : IPieceState 
{
    boolean validateMove(int startIndex, int endIndex) 
    {
        /* Here we will check if the requested move is valid.
           Pawn moves: index + 8 (black)
                       index + 9 (black)
                       index + 7 (black)

                       index - 8 (white)
                       index - 9 (white)
                       index - 7 (white)

            First move: +- 8 or +- 16

        */
        if(endIndex >= startIndex + 7 && endIndex <= startIndex + 9)
        return false;
    }
}

class KnightState : IPieceState 
{
    boolean validateMove(int startIndex, int endIndex) 
    {
        // Here we will check if the requested move is valid.
        return false;
    }
}

class BishopState : IPieceState
{
    boolean validateMove(int startIndex, int endIndex) 
    {
        // Here we will check if the requested move is valid.
        return false;
    }
}

class RookState : IPieceState 
{
    boolean validateMove(int startIndex, int endIndex) 
    {
        // Here we will check if the requested move is valid.
        return false;
    }
}

class QueenState : IPieceState
{
    boolean validateMove(int startIndex, int endIndex) 
    {
        // Here we will check if the requested move is valid.
        return false;
    }
}

class KingState : IPieceState 
{
    boolean validateMove(int startIndex, int endIndex) 
    {
        // Here we will check if the requested move is valid.
        return false;
    }
}