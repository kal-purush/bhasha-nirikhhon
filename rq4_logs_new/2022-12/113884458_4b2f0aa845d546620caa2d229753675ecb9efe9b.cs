using System.Drawing;

namespace _2022.Days;

public class Day8 : Day
{
    private int _y = 0;
    private readonly HashSet<Point> _visibleTrees = new();

    private readonly Dictionary<int, int> _highestSeenYStartHeight = new();
    private readonly Dictionary<int, LinkedList<(int height, Point pos)>> _currentVisibleYEndHeights = new();

    private readonly Dictionary<int, Dictionary<int, int>> _numVisibleNorthTreesPerX = new();
    private readonly Dictionary<int, List<Beauty>> _activeSouthBeautiesByX = new();
    private readonly Dictionary<Point, Beauty> _beauties = new();

    public Day8() : base(8)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        var maxCoord = line.Length - 1;

        var highestSeenXHeight = 0;
        var currentVisibleEndXHeights = new LinkedList<(int height, Point pos)>();

        var visibleWestTrees = GenerateInitialVisibleTrees();
        var activeEastBeauties = new List<Beauty>();
        
        for (var x = 0; x <= maxCoord; x++)
        {
            var heightStr = line[x].ToString();

            if (!int.TryParse(heightStr, out var height))
                throw new ArgumentException($"Expected number, got {heightStr}");

            var tree = new Point(x, this._y);

            if (this._y == 0)
            {
                ProcessFirstLine(tree, x, maxCoord, height);
            }
            else if (this._y == maxCoord)
            {
                ProcessLastLine(x, maxCoord, tree, height);
            }
            else if (x == 0)
            {
                highestSeenXHeight = height;
                ProcessStartColumnTree(tree);
            }
            else if (x == maxCoord)
            {
                ProcessEndColumnTree(currentVisibleEndXHeights, height, tree, activeEastBeauties);
            }
            else
            {
                highestSeenXHeight = ProcessInnerTree(height, highestSeenXHeight, tree, x, visibleWestTrees, ref currentVisibleEndXHeights, ref activeEastBeauties);
            }
        }
        
        this._y++;
    }

    private void ProcessFirstLine(Point tree, int x, int maxCoord, int height)
    {
        // First line
        this._visibleTrees.Add(tree);

        if (x == 0 || x == maxCoord)
        {
            // Top left/right corner
        }
        else
        {
            // First line, one of the start trees
            this._highestSeenYStartHeight.Add(x, height);
            this._currentVisibleYEndHeights.Add(x,
                new LinkedList<(int height, Point pos)>(new (int height, Point pos)[] { (height, tree) }));

            // Part 2
            var visibleNorthTrees = GenerateInitialVisibleTrees();
            this._numVisibleNorthTreesPerX.Add(x, visibleNorthTrees);
            this._activeSouthBeautiesByX.Add(x, new List<Beauty>());
        }
    }
    
    private void ProcessLastLine(int x, int maxCoord, Point tree, int height)
    {
        // Last line
        if (x == 0 || x == maxCoord)
        {
            // Bottom left/right corner
            this._visibleTrees.Add(tree);
        }
        else
        {
            // Last line, one of the end trees
            var treesVisibleFromBottom = this._currentVisibleYEndHeights[x];

            BadlyNamedProcessEndTree(treesVisibleFromBottom, (height, tree));

            foreach (var (_, visibleEndTree) in treesVisibleFromBottom)
            {
                this._visibleTrees.Add(visibleEndTree);
            }

            // Part 2
            foreach (var activeBeauty in this._activeSouthBeautiesByX[x])
            {
                activeBeauty.SouthScore++;
            }
        }
    }
    
    private void ProcessStartColumnTree(Point tree) 
    {
        // First tree
        this._visibleTrees.Add(tree);
    }

    private void ProcessEndColumnTree(LinkedList<(int height, Point pos)> currentVisibleEndXHeights, int height, Point tree, IEnumerable<Beauty> activeEastBeauties)
    {
        // Last tree
        BadlyNamedProcessEndTree(currentVisibleEndXHeights, (height, tree));

        foreach (var (_, visibleEndTree) in currentVisibleEndXHeights)
        {
            this._visibleTrees.Add(visibleEndTree);
        }

        // Part 2
        foreach (var activeBeauty in activeEastBeauties)
        {
            activeBeauty.EastScore++;
        }
    }
    
    private int ProcessInnerTree(int height, int highestSeenXHeight, Point tree, int x, IDictionary<int, int> visibleWestTrees,
        ref LinkedList<(int height, Point pos)> currentVisibleEndXHeights, ref List<Beauty> activeEastBeauties)
    {
        // Some middle tree

        // Part 1
        // x
        if (height > highestSeenXHeight)
        {
            this._visibleTrees.Add(tree);

            highestSeenXHeight = height;
            currentVisibleEndXHeights =
                new LinkedList<(int height, Point pos)>(new (int height, Point pos)[] { (height, tree) });
        }
        else
        {
            BadlyNamedProcessEndTree(currentVisibleEndXHeights, (height, tree));
        }

        // y
        if (height > this._highestSeenYStartHeight[x])
        {
            this._visibleTrees.Add(tree);

            this._highestSeenYStartHeight.Remove(x);
            this._highestSeenYStartHeight.Add(x, height);
            this._currentVisibleYEndHeights.Remove(x);
            this._currentVisibleYEndHeights.Add(x,
                new LinkedList<(int height, Point pos)>(new (int height, Point pos)[] { (height, tree) }));
        }
        else
        {
            BadlyNamedProcessEndTree(this._currentVisibleYEndHeights[x], (height, tree));
        }

        // Part 2
        var visibleNorthTrees = this._numVisibleNorthTreesPerX[x];

        var beauty = new Beauty(height)
        {
            NorthScore = visibleNorthTrees[height],
            WestScore = visibleWestTrees[height]
        };

        this._beauties.Add(tree, beauty);
        
        UpdateVisibleTreesInDirection(visibleNorthTrees, height);
        UpdateVisibleTreesInDirection(visibleWestTrees, height);

        var newActiveEastBeauties = new List<Beauty>();

        foreach (var activeBeauty in activeEastBeauties)
        {
            activeBeauty.EastScore++;

            if (activeBeauty.TreeHeight > height)
            {
                newActiveEastBeauties.Add(activeBeauty);
            }
        }

        activeEastBeauties = newActiveEastBeauties;
        activeEastBeauties.Add(beauty);

        var activeSouthBeauties = this._activeSouthBeautiesByX[x];
        var newActiveSouthBeauties = new List<Beauty>();

        foreach (var activeBeauty in activeSouthBeauties)
        {
            activeBeauty.SouthScore++;

            if (activeBeauty.TreeHeight > height)
            {
                newActiveSouthBeauties.Add(activeBeauty);
            }
        }

        newActiveSouthBeauties.Add(beauty);

        this._activeSouthBeautiesByX.Remove(x);
        this._activeSouthBeautiesByX.Add(x, newActiveSouthBeauties);
        
        return highestSeenXHeight;
    }

    private static void BadlyNamedProcessEndTree(LinkedList<(int height, Point pos)> trees, (int height, Point pos) newTree)
    {
        var lastTree = trees.Last;

        while (lastTree is not null)
        {
            if (lastTree.Value.height <= newTree.height)
            {
                // Will be hidden by the new tree, so remove.
                lastTree = lastTree.Previous;
                trees.RemoveLast();
            }
            else
            {
                break;
            }
        }

        trees.AddLast(newTree);
    }

    private static Dictionary<int, int> GenerateInitialVisibleTrees()
    {
        var visibleTreesInDirection = new Dictionary<int, int>();

        for (var i = 0; i <= 9; i++)
        {
            visibleTreesInDirection.Add(i, 1);
        }

        return visibleTreesInDirection;
    }

    private static void UpdateVisibleTreesInDirection(IDictionary<int, int> visibleTrees, int newTreeHeight)
    {
        for (var i = 0; i <= 9; i++)
        {
            var newValue = i <= newTreeHeight ? 1 : (visibleTrees[i] + 1);
            visibleTrees.Remove(i);
            visibleTrees.Add(i, newValue);
        }
    }

    public override void SolvePart1()
    {
        this.Part1Solution = this._visibleTrees.Count.ToString();
    }

    public override void SolvePart2()
    {
        this.Part2Solution = this._beauties.Values.Max(b => b.GetScenicScore()).ToString();
    }

    private class Beauty
    {
        public readonly int TreeHeight;
        
        public int NorthScore;
        public int SouthScore;
        public int WestScore;
        public int EastScore;

        public Beauty(int treeHeight)
        {
            TreeHeight = treeHeight;
        }

        public int GetScenicScore()
        {
            return this.NorthScore * this.SouthScore * this.WestScore * this.EastScore;
        }
    }
}