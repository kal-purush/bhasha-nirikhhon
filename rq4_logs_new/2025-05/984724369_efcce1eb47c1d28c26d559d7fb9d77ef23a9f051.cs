using System.Text;
using System.Text.RegularExpressions;
using DirectoryProject.DirectoryService.Domain.Shared;
using Microsoft.EntityFrameworkCore;

namespace DirectoryProject.DirectoryService.Domain.DepartmentValueObjects;

public class DepartmentPath
{
    public LTree Value { get; }

    public static DepartmentPath CreateFromExisting(LTree existingPath)
    {
        return new DepartmentPath(existingPath);
    }

    public static DepartmentPath CreateFromStringAndParent(string stringToSlug, string? parentPath)
    {
        // replace all white spaces with -
        stringToSlug = new Regex(@"\s+").Replace(stringToSlug.Trim().ToLower(), "-");

        // replace all characters, specified in settings
        var sb = new StringBuilder();
        foreach (var ch in stringToSlug)
        {
            if (_replaceChars.TryGetValue(ch, out var replacement))
                sb.Append(replacement);
            else
                sb.Append(ch);
        }
        stringToSlug = sb.ToString();

        // remove all characters, except lower case latin, digits, '.' and '-'
        stringToSlug = new Regex(@"[^a-z0-9\-\.]").Replace(stringToSlug, string.Empty);

        if (parentPath is null)
            return new DepartmentPath(stringToSlug);
        else
            return new DepartmentPath($"{parentPath}.{stringToSlug}");
    }

    private static Dictionary<char, string> _replaceChars = [];
    public static UnitResult SetReplaceChars(Dictionary<char, string> replaceChars)
    {
        if (replaceChars.Count != 0)
            return Error.Failure("application.failure", "Could not set _replaceChars again");

        _replaceChars = replaceChars;
        return UnitResult.Success();
    }

    private DepartmentPath(string value)
    {
        Value = value;
    }

    // EF Core
    private DepartmentPath() { }
}