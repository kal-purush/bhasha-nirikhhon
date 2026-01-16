namespace AdventOfCode.Days;

public class Day07 : BaseDay
{
    private readonly string[] _input;
    private readonly Directory _baseDirectory = new Directory
    {
        DirectoryName = "/",
    };
    private readonly List<long> _directorySizes = new List<long>();
    private readonly Dictionary<string, Directory> _directories = new Dictionary<string, Directory>();

    public Day07()
    {
        _input = File.ReadAllLines(InputFilePath).Select(x => x.Replace("$ ", "")).ToArray();

        //_directories.Add("/", );
    }

    public override ValueTask<string> Solve_1()
    {
        var lsExecuted = false;
        var currentDirectory = _baseDirectory;
        var directorySizes = new List<long>();
        var iteration = 1;
        foreach (var command in _input.Skip(1))
        {
            iteration++;

            if (command[..2] == "cd")
            {
                lsExecuted = false;

                if (command[3..] == "..")
                {
                    directorySizes.Add(currentDirectory.TotalDirectorySize);
                    currentDirectory = currentDirectory.ParentDirectory;
                }
                else
                {
                    //currentDirectory = _directories[$"{currentDirectory.DirectoryName}_{command[3..]}"];
                    currentDirectory = currentDirectory.Directories.Where(x => x.DirectoryName == command[3..]).FirstOrDefault();
                }

                continue;
            }

            if (command[..2] == "ls")
            {
                lsExecuted = true;
                continue;
            }

            if (lsExecuted)
            {
                if (command[..3] == "dir")
                {
                    var directoryName = command[4..];
                    var newDirectory = new Directory
                    {
                        DirectoryName = directoryName,
                        ParentDirectory = currentDirectory,
                    };

                    currentDirectory.Directories.Add(newDirectory);

                    if (!_directories.ContainsKey($"{currentDirectory.DirectoryName}_{newDirectory.DirectoryName}"))
                    {
                        _directories.Add($"{currentDirectory.DirectoryName}_{newDirectory.DirectoryName}", newDirectory);
                    }
                }
                else
                {
                    var file = command.Split(" ");
                    var directoryFile = new DirectoryFile
                    {
                        FileSize = long.Parse(file[0]),
                        FileName = file[1]
                    };

                    currentDirectory.Files.Add(directoryFile);
                }
            }
        }

        FindDirectorySizes(_baseDirectory);
        _directorySizes.Sort();

        var total = directorySizes.Where(x => x <= 100000).Sum();

        return new(total.ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        var spaceUsed = 70000000 - _baseDirectory.TotalDirectorySize;
        var spaceNeededToFree = 30000000 - spaceUsed;
        var directorySize = _directorySizes.Where(x => x >= spaceNeededToFree).First();

        return new(directorySize.ToString());
    }

    private void FindDirectorySizes(Directory directory)
    {
        foreach (var dir in directory.Directories)
        {
            _directorySizes.Add(dir.TotalDirectorySize);
            FindDirectorySizes(dir);
        }
    }
}

public class Directory
{
    public string DirectoryName { get; set; }
    public List<DirectoryFile> Files { get; set; } = new List<DirectoryFile>();
    public long TotalFileSize => Files.Sum(f => f.FileSize);
    public long TotalDirectorySize => TotalFileSize + Directories.Sum(x => x.TotalDirectorySize);
    public Directory ParentDirectory { get; set; }
    public List<Directory> Directories { get; set; } = new List<Directory>();
}

public class DirectoryFile
{
    public string FileName { get; set; }
    public long FileSize { get; set; }
}