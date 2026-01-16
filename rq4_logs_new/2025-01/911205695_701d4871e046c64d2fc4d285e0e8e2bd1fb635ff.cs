using System.Collections.Generic;
using Godot;

public static class StringManager
{
    static Dictionary<string, StringName> _stringNames = new Dictionary<string, StringName>();

    // Some Godot string cause the creation of StringName every frame
    // this seems to leak memory, possibly, because Godot doesn't like to reclaim it, but 
    // allocates happily every frame
    public static StringName S(string str)
    {
        if (!_stringNames.ContainsKey(str))
        {
            _stringNames[str] = new StringName(str);
        }

        return _stringNames[str];
    }
}