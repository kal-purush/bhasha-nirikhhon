using Microsoft.Xna.Framework;

class Tile : Object3D
{
    public Point gridPosition;

    public Tile(string modelName, string id) : base(modelName, id)
    {

    }
}

//class EntryTile : Tile
//{
//    public EntryTile()
//    {

//    }
//}

//class ExitTile : Tile
//{
//    public ExitTile(Point point)
//    {
//        gridPosition = point;
//    }
//}

//class MainPathTile : Tile
//{
//    public MainPathTile(Point point)
//    {
//        gridPosition = point;
//    }
//}

//class SidePathEntryTile : Tile
//{
//    public SidePathEntryTile(Point point)
//    {
//        gridPosition = point;
//    }
//}

//class SidePathTile : Tile
//{
//    public SidePathTile(Point point)
//    {
//        gridPosition = point;
//    }
//}