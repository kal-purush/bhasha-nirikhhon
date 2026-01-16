using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;

//A randomly generated level
class RandomLevel : Level
{
    private Dictionary<Point, Tile> tileList; //List of tiles, accessible by a position
    private List<Point> keyList; //List of all occupied positions
    private Tile newTile; //Next tile to be build
    private Point position; 
    private Random random;
    private int tiles; //Amount of tiles to indicate the size of the level
   
    //Gives the tileList
    public Dictionary<Point, Tile> TileList
    {
        get { return tileList; }
    }

    //Gives the keyList
    public List<Point> KeyList
    {
        get
        {

            return keyList;
        }
    }

    //making the list of tiles into a grid
    public TileGrid Grid //auteur: Wouter
    {
        get
        {
            TileGrid grid;

            //Step 1: figuring out the size of the grid
            Point lowestPoint = keyList[0];
            Point highestPoint = keyList[0];
            foreach (Point point in keyList)
            {
                if (point.X < lowestPoint.X)
                {
                    lowestPoint.X = point.X;
                }
                if (point.Y < lowestPoint.Y)
                {
                    lowestPoint.Y = point.Y;
                }
                if (point.X > highestPoint.X)
                {
                    highestPoint.X = point.X;
                }
                if (point.Y > highestPoint.Y)
                {
                    highestPoint.Y = point.Y;
                }
            }

            grid = new TileGrid(highestPoint.X - lowestPoint.X + 1, highestPoint.Y - lowestPoint.Y + 1, "Grid");
            
            //Step 2: filling the grid
            foreach (Point position in keyList)
            {
                grid.Add(tileList[position], position.X - lowestPoint.X, position.Y - lowestPoint.Y);
            }

            //Step 3: returning the grid
            return grid;
        }
    }

    //Creating/generating the level itself
    public RandomLevel(int tiles = 10)
    {
        //Assining the variables
        tileList = new Dictionary<Point, Tile>();
        keyList = new List<Point>();
        newTile = new EntryTile();
        random = new Random();
        this.tiles = tiles;

        //Create the startpoint
        newTile.tilePosition = Point.Zero;
        position = newTile.tilePosition;
        tileList.Add(position, newTile);
        keyList.Add(position);

        //Creating the paths
        CreateMainPath();

        for (int i = random.Next(1, tiles *3); i > 0; i--)
            CreateSidePath(random.Next(3, tiles / 2));

        TileGrid tileGrid = Grid;
        gameObjects.Add(tileGrid);

        player = new Player(new Vector3(tileGrid.Columns * 200 /2,0, tileGrid.Rows * 200 /2 ));
        gameObjects.Add(player);
    }

    private void CreateMainPath()
    {
        //Creating all MainPathTiles
        while (tiles >= 0)
        {
            List<Point> possiblePositions = new List<Point>();

            //Look where a Tile can be placed
            Point position = new Point(newTile.tilePosition.X - 1, newTile.tilePosition.Y);

            if (CanPlaceMainPathTile(position))
                possiblePositions.Add(position);

            position.X = newTile.tilePosition.X;
            position.Y = newTile.tilePosition.Y + 1;

            if (CanPlaceMainPathTile(position))
                possiblePositions.Add(position);

            position.X = newTile.tilePosition.X + 1;
            position.Y = newTile.tilePosition.Y;

            if (CanPlaceMainPathTile(position))
                possiblePositions.Add(position);

            position.X = newTile.tilePosition.X;
            position.Y = newTile.tilePosition.Y - 1;

            if (CanPlaceMainPathTile(position))
                possiblePositions.Add(position);

            //Pick where to place it
            int i = random.Next(0, possiblePositions.Count);

            if (tiles > 0)
            {
                //Choose where to place the Tile
                try
                {
                    newTile = CreateMainPathTile(possiblePositions[i]);
                    tileList.Add(newTile.tilePosition, newTile);
                }
                catch
                {
                    //Mainpath enclosed itself, ending it here
                    Console.WriteLine("MainPathError");
                    possiblePositions = GetPossiblePositions(newTile.tilePosition);
                    newTile = CreateExitTile(possiblePositions[i]);
                    tileList.Add(newTile.tilePosition, newTile);
                    tiles = -2;
                }

            }
            else
            {
                //Choose where to place the Tile
                try
                {
                    newTile = CreateExitTile(possiblePositions[i]);
                    tileList.Add(newTile.tilePosition, newTile);
                }
                catch (ArgumentOutOfRangeException e)
                {
                    //Couldn't place exit, so place it at the last placed tile
                    newTile = CreateExitTile(keyList[keyList.Count - 1]);
                    tileList[keyList[keyList.Count - 1]] = newTile;
                    // Console.WriteLine("X:" + newTile.tilePosition.X + "\nY:" + newTile.tilePosition.Y + "");
                    tiles--;
                    continue;
                }
                
            }

            keyList.Add(newTile.tilePosition);
/*            possiblePositions.RemoveAt(i);

            foreach (Point element in possiblePositions)
            {
                tileList.Add(element, new WallTile(element));
                keyList.Add(element);
            }
*/
           // Console.WriteLine("X:" + newTile.tilePosition.X + "\nY:" + newTile.tilePosition.Y + "");

            tiles--;
        }

    }


    //Creating SidePath From here
    private void CreateSidePath(int tiles)
    {
        //Pick a place to place a SidePath
        Point nextPosition;
        do
        {
            nextPosition = keyList[random.Next(0, keyList.Count - 1)];
        } while (tileList[nextPosition] is ExitTile);

        Tile nextTile = CanCreateSidePath(nextPosition);

        //If it can be placed
        if (nextTile != null)
        {

            tileList.Add(nextTile.tilePosition, nextTile);
            keyList.Add(nextTile.tilePosition);

            while (tiles > 0)
            {
                List<Point> possiblePositions = GetPossiblePositions(nextTile.tilePosition);

                if (possiblePositions.Count > 0)
                {
                    //Choose where to place the Tile
                    nextTile = new SidePathTile(possiblePositions[random.Next(0, possiblePositions.Count - 1)]);
                    tileList.Add(nextTile.tilePosition, nextTile);
                    keyList.Add(nextTile.tilePosition);
                   // Console.WriteLine("Side: \nX: " + nextTile.tilePosition.X + "\nY: " + nextTile.tilePosition.Y);
                }
                else
                {
                    break;
                }
                tiles--;
            }

        }
        //Try again
        else
        {
            CreateSidePath(tiles);
        }
    }

    //Checks whether the place is suitable for a SidePath
    private Tile CanCreateSidePath(Point nextPosition)
    {
        List<Point> possibleEntrys = new List<Point>();
        Point test = nextPosition;

        test.X = nextPosition.X - 1;
        if (CanPlaceSideEntry(test))
        {
            possibleEntrys.Add(test);
        }
        test.X = nextPosition.X + 1;
        if (CanPlaceSideEntry(test))
        {
            possibleEntrys.Add(test);
        }
        test.X = nextPosition.X;
        test.Y = nextPosition.Y - 1;
        if (CanPlaceSideEntry(test))
        {
            possibleEntrys.Add(test);
        }
        test.Y = nextPosition.Y + 1;
        if (CanPlaceSideEntry(test))
        {
            possibleEntrys.Add(test);
        }

        try
        {
            //If it is suitable
            return (new SidePathEntryTile(possibleEntrys[random.Next(0, possibleEntrys.Count)]));
        }
        catch(Exception e)
        {
            //If it is, for some reason, unsuitable
            Console.WriteLine("SidePathEntryError: " + e.StackTrace);
            return null;
        }
    }
    
    //Can a SidePathEntryTile be placed here
    private bool CanPlaceSideEntry(Point currentPosition)
    {
        return (!tileList.ContainsKey(currentPosition) && GetPossiblePositions(currentPosition).Count > 0);
    }

    //Checking all the empty tiles around the position
    private List<Point> GetPossiblePositions(Point currentPosition)
    {
        List<Point> possiblePositions = new List<Point>();

        Point position = new Point(currentPosition.X - 1, currentPosition.Y);

        if (!tileList.ContainsKey(position))
            possiblePositions.Add(position);

        position.X = currentPosition.X;
        position.Y = currentPosition.Y + 1;

        if (!tileList.ContainsKey(position))
            possiblePositions.Add(position);

        position.X = currentPosition.X + 1;
        position.Y = currentPosition.Y;

        if (!tileList.ContainsKey(position))
            possiblePositions.Add(position);

        position.X = currentPosition.X;
        position.Y = currentPosition.Y - 1;

        if (!tileList.ContainsKey(position))
            possiblePositions.Add(position);

        return possiblePositions;

    }

    //Checks whether a MainPathTile can be placed here
    private bool CanPlaceMainPathTile(Point currentPosition)
    {
        return (!tileList.ContainsKey(currentPosition) && GetPossiblePositions(currentPosition).Count == 3);
    }

    //Create a MainPathTile (this method can still be replaced with 'new MainPathTile(point)')
    private Tile CreateMainPathTile(Point point)
    {
        return new MainPathTile(point);
    }

    //Create a ExitTile (this method can still be replaced with 'new ExitTile(point)') 
    private Tile CreateExitTile(Point point)
    {
        return new ExitTile(point);
    }

}