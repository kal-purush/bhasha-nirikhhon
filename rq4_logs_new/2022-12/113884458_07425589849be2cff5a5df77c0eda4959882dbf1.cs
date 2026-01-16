using System.Text.RegularExpressions;
using _2022.Utils;
using Directory = _2022.Utils.Directory;

namespace _2022.Days;

public class Day7 : Day
{
    private readonly AocComputer _comp = new();
    
    private const int _totalDiskSpace = 70000000;
    private const int _requiredFreeDiskSpace = 30000000;
    
    public Day7() : base(7)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        this._comp.ProcessTerminalLine(line);
    }

    public override void SolvePart1()
    {
        var totalSmallDirSize = 0;
        
        var queue = new Queue<Directory>();
        
        queue.Enqueue(this._comp.GetRoot());

        while (queue.TryDequeue(out var dir))
        {
            if (dir.GetSize() <= 100000)
            {
                totalSmallDirSize += dir.GetSize();
            }

            var directoryChildren = dir.GetDirectoryChildren();

            foreach (var subDir in directoryChildren)
            {
                queue.Enqueue(subDir);
            }
        }

        this.Part1Solution = totalSmallDirSize.ToString();
    }

    public override void SolvePart2()
    {
        var root = this._comp.GetRoot();
        
        var curFreeSpace = _totalDiskSpace - root.GetSize();

        var minSpaceToFree = _requiredFreeDiskSpace - curFreeSpace;

        var candidateToDelete = root;

        var queue = new Queue<Directory>(root.GetDirectoryChildren());

        while (queue.TryDequeue(out var dir))
        {
            var dirSize = dir.GetSize();

            if (dirSize < minSpaceToFree)
            {
                continue;
            }

            if (dirSize < candidateToDelete.GetSize())
            {
                candidateToDelete = dir;
            }

            var childDirs = dir.GetDirectoryChildren();

            foreach (var subDir in childDirs)
            {
                queue.Enqueue(subDir);
            }
        }

        this.Part2Solution = candidateToDelete.GetSize().ToString();
    }
}