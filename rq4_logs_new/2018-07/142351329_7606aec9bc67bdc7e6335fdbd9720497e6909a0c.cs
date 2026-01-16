using System;
using GenderChess.CODE;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

public class startBoard
{
    private List<Troop> W = new List<Troop>();
    private List<Troop> B = new List<Troop>();

    public GameDataManager startBoard()
    {
        createWList();
        createBList();
        return new GameDataManager(8, 8, true, W, B);

    }
    private createWList()
    {
        addPawns(true, 2);
        addKnight(true,1);
        addBishop(true, 1);
        addRook(true, 1);
        addQueen(true, 1);
        addKing(true, 1);
    }
    private createBList()
    {
        addPawns(false, 7);
        addKnight(false,8);
        addBishop(false, 8);
        addRook(false, 8);
        addQueen(false, 8);
        addKing(false, 8);

    }

    private addPawns(bool color, int line)
    {
        if (color)
        {
            for (int i = 8; i != 0; i--)
            {
                W.Add(new Troop(color, UnitTypes.Pawn,/*gender*/null, new Coords(line, i)));
            }
        }
        else
        {
            for (int i = 8; i != 0; i--)
            {
                B.Add(new Troop(color, UnitTypes.Pawn,/*gender*/null, new Coords(line, i)));
            }
        }
    }

    private addKnight(bool color, int line)
    {
        if (color)
        {
            for (int i = 2; i <= 7; i += 5)
            {
                W.Add(new Troop(color, UnitTypes.Knight,/*gender*/null, new Coords(line, i)));
            }
        }
        else
        {
            for (int i = 2; i <= 7; i += 5)
            {
                W.Add(new Troop(color, UnitTypes.Knight,/*gender*/null, new Coords(line, i)));
            }
        }
    }

    private addBishop(bool color, int line)
    {
        if (color)
        {
            for (int i = 3; i != 9; i += 3)
            {
                W.Add(new Troop(color, UnitTypes.Bishop,/*gender*/null, new Coords(line, i)));
            }
        }
        else
        {
            for (int i = 3; i != 9; i += 3)
            {
                W.Add(new Troop(color, UnitTypes.Bishop,/*gender*/null, new Coords(line, i)));
            }
        }
    }

    private addRook(bool color, int line)
    {
        if (color)
        {
            for (int i = 1; i >= 8; i += 7)
            {
                W.Add(new Troop(color, UnitTypes.Rook,/*gender*/null, new Coords(line, i)));
            }
        }
        else
        {
            for (int i = 1; i >= 8; i += 7)
            {
                B.Add(new Troop(color, UnitTypes.Rook,/*gender*/null, new Coords(line, i)));
            }
        }
    }

    private addQueen(bool color, int line)
    {
        if (color)
        {
            W.Add(new Troop(color, UnitTypes.Queen,/*gender*/null, new Coords(line, i)));
        }
        else
        {
            B.Add(new Troop(color, UnitTypes.Queen,/*gender*/null, new Coords(line, i)));
        }
    }

    private addKing(bool color, int line)
    {
        if (color)
        {
            W.Add(new Troop(color, UnitTypes.Queen,/*gender*/null, new Coords(line, i)));
        }
        else
        {
            B.Add(new Troop(color, UnitTypes.Queen,/*gender*/null, new Coords(line, i)));
        }
    }


}