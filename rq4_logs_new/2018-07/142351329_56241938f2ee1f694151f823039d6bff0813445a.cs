using System;
using GenderChess.CODE;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

public class startBoard
{
    public static List<Troop> W = new List<Troop>();
    public static List<Troop> B = new List<Troop>();
    public static List<GenderTypes> BTypes = new List<GenderTypes>(ChessStuff.ChessRules.gendersList);
    public static List<GenderTypes> WTypes = new List<GenderTypes>(ChessStuff.ChessRules.gendersList);

    public static void createWList()
    {
        addPawns(true, 1);
        addKnight(true,0);
        addBishop(true, 0);
        addRook(true, 0);
        addQueen(true, 0);
        addKing(true, 0);
    }
    public static void createBList()
    {
        addPawns(false, 6);
        addKnight(false,7);
        addBishop(false, 7);
        addRook(false, 7);
        addQueen(false, 7);
        addKing(false, 7);

    }
    static Random random = new Random();
    public static void addPawns(bool color, int line)
    {
        if (color)
        {
            for (int i = 7; i != -1; i--)
            {
                W.Add(new Troop(color, UnitTypes.Pawn, WGetGender(), new Coords(i, line)));
            }
        }
        else
        {
            for (int i = 7; i != -1; i--)
            {
                B.Add(new Troop(color, UnitTypes.Pawn, BGetGender(), new Coords(i, line)));
            }
        }
    }

    public static GenderTypes WGetGender()
    {
        int index = random.Next(WTypes.Count);
        GenderTypes output = WTypes[index];
        WTypes.RemoveAt(index);
        return output;
    }

    public static GenderTypes BGetGender()
    {
        int index = random.Next(BTypes.Count);
        GenderTypes output = BTypes[index];
        BTypes.RemoveAt(index);
        return output;
    }

    public static void addKnight(bool color, int line)
    {
        if (color)
        {
            for (int i = 1; i <= 6; i += 5)
            {
                W.Add(new Troop(color, UnitTypes.Knight, WGetGender(), new Coords(i, line)));
            }
        }
        else
        {
            for (int i = 1; i <= 6; i += 5)
            {
                W.Add(new Troop(color, UnitTypes.Knight, BGetGender(), new Coords(i, line)));
            }
        }
    }

    public static void addBishop(bool color, int line)
    {
        if (color)
        {
            for (int i = 2; i <= 5; i += 3)
            {
                W.Add(new Troop(color, UnitTypes.Bishop, WGetGender(), new Coords(i, line)));
            }
        }
        else
        {
            for (int i = 2; i <= 5; i += 3)
            {
                W.Add(new Troop(color, UnitTypes.Bishop, BGetGender(), new Coords(i, line)));
            }
        }
    }

    public static void addRook(bool color, int line)
    {
        if (color)
        {
            for (int i = 0; i <= 8; i += 7)
            {
                W.Add(new Troop(color, UnitTypes.Rook, WGetGender(), new Coords(i, line)));
            }
        }
        else
        {
            for (int i = 0; i <= 8; i += 7)
            {
                B.Add(new Troop(color, UnitTypes.Rook, BGetGender(), new Coords(i, line)));
            }
        }
    }

    public static void addQueen(bool color, int line)
    {
        if (color)
        {
            W.Add(new Troop(color, UnitTypes.Queen, WGetGender(), new Coords(3, line)));
        }
        else
        {
            B.Add(new Troop(color, UnitTypes.Queen, BGetGender(), new Coords(4, line)));
        }
    }

    public static void addKing(bool color, int line)
    {
        if (color)
        {
            W.Add(new Troop(color, UnitTypes.Queen, GenderTypes.Default, new Coords(3, line)));
        }
        else
        {
            B.Add(new Troop(color, UnitTypes.Queen, GenderTypes.Default, new Coords(4, line)));
        }
    }


}