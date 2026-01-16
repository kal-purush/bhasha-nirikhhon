using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Bl.QueryVisitor;

internal class ParamDictionary
    : IReadOnlyDictionary<string, object?>
{
    private readonly Dictionary<string, object?> _dictionary;

    public object? this[string key] => _dictionary[key];

    public IEnumerable<string> Keys => _dictionary.Keys;

    public IEnumerable<object?> Values => _dictionary.Values;

    public int Count => _dictionary.Count;

    private int _lastId { get; set; }

    public ParamDictionary()
    {
        _dictionary = new();
        _lastId = 1000;
    }

    public void Clear()
    {
        _lastId = 1000;
        _dictionary.Clear();
    }

    public bool ContainsKey(string key)
    {
        return _dictionary.ContainsKey(key);
    }

    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
    {
        return _dictionary.GetEnumerator();
    }

    public bool TryGetValue(string key, [MaybeNullWhen(false)] out object? value)
    {
        return _dictionary.TryGetValue(key, out value);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return _dictionary.GetEnumerator();
    }

    /// <summary>
    /// Add next parameter
    /// </summary>
    /// <param name="value">Value from key</param>
    /// <returns>Key/Parameter name added</returns>
    public string AddNextParam(object? value)
    {
        var key = "@P" + _lastId;

        _dictionary.Add(key, value);

        _lastId++;

        return key;
    }
}