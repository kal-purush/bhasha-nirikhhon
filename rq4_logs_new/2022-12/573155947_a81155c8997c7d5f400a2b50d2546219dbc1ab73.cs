namespace AdventOfCode.Day07;

public class Day07 : Day
{
    
    public override ValueTask<string> Solve_1()
    {
        var disk = Parse(new DiskItem(Type.Directory, new Dictionary<string, DiskItem>(), null, "/"), _input.SplitNewLine());

        var b = EnumerateDisk(disk).Select(x => x.Name).ToList();
        return new(EnumerateDisk(disk).Where(x => x.Type == Type.Directory).Where(disk => disk.CalculatedSize < 100000).Sum(x => x.CalculatedSize).ToString());
    }

    private IEnumerable<DiskItem> EnumerateDisk(DiskItem disk)
    {
        yield return disk;

        if (disk.Type == Type.Directory)
        {
            foreach (var subItem in disk.SubItem.Values)
            {
                foreach (var subItem2 in EnumerateDisk(subItem))
                {
                    yield return subItem2;
                }
            }
        }
      

    }

    private DiskItem Parse(DiskItem diskItem, string[] splitNewLine)
    {
        var activeDiskItem = diskItem;
        foreach (var line in splitNewLine)
        {
            if (line.StartsWith("$"))
            {
                if (line.StartsWith("$ cd /"))
                {
                    // Reset active
                    activeDiskItem = diskItem;
                }
                else if (line.StartsWith("$ cd .."))
                {
                    activeDiskItem = activeDiskItem.Parent;
                }
                else if (line.StartsWith("$ cd "))
                {
                    // Reset active

                    var newItem = new DiskItem(Type.Directory, new Dictionary<string, DiskItem>(), activeDiskItem, line.Substring("$ cd ".Length));
                    if (!activeDiskItem.SubItem.ContainsKey(newItem.Name))
                    {
                        activeDiskItem.SubItem.Add(newItem.Name, newItem);
                    }

                    activeDiskItem = activeDiskItem.SubItem[newItem.Name];
                }
                else if (line.StartsWith("$ ls"))
                {
                    // Ignore
                }
            }
            else
            {
                if (line.StartsWith("dir "))
                {
                    var newItem = new DiskItem(Type.Directory, new Dictionary<string, DiskItem>(), activeDiskItem, line.Substring("dir ".Length));
                    if (!activeDiskItem.SubItem.ContainsKey(newItem.Name))
                    {
                        activeDiskItem.SubItem.Add(newItem.Name, newItem);
                    }
                }
                else
                {
                    var newItem = new DiskItem(Type.File, new Dictionary<string, DiskItem>(), activeDiskItem, line.SplitSpace()[1], line.SplitSpace()[0]?.AsLong());
                    if (!activeDiskItem.SubItem.ContainsKey(newItem.Name))
                    {
                        activeDiskItem.SubItem.Add(newItem.Name, newItem);
                    }
                }
                // LS action
                //Action
            }
        }


        return diskItem;
    }

    public override ValueTask<string> Solve_2()
        => throw new NotImplementedException();
}

public enum Type
{
    Directory,
    File,
}

public record DiskItem(Type Type, IDictionary<string, DiskItem> SubItem, DiskItem Parent, string Name, long? Size = null)
{
    public long CalculatedSize => Type == Type.File ? this.Size.Value : SubItem.Values.Sum(x => x.CalculatedSize);
};