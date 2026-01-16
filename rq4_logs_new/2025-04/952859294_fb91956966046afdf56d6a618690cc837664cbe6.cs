namespace TAmeAtCoderLibrary;

using System;
using System.Collections.Generic;

/// <summary>
/// AVL木を使用して、キーの順序を維持するマップの実装です。
/// このクラスは辞書の高速な検索とAVL木の順序付き走査機能を組み合わせています。
/// </summary>
/// <typeparam name="TKey">マップ内のキーの型。IComparableを実装している必要があります。</typeparam>
/// <typeparam name="TValue">マップ内の値の型。</typeparam>
public class OrderedKeyMap<TKey, TValue> where TKey : IComparable<TKey>
{
    private readonly AvlTree<TKey> _avl = new();
    private readonly Dictionary<TKey, TValue> _dic = new();

    /// <summary>最小のキーを取得します</summary>
    public TKey MinKey => _avl.MinKey;
    /// <summary>最大のキーを取得します</summary>
    public TKey MaxKey => _avl.MaxKey;
    /// <summary>最小のキーに対応する値を取得します</summary>
    public TValue MinKeyValue => _dic[_avl.MinKey];
    /// <summary>最大のキーに対応する値を取得します</summary>
    public TValue MaxKeyValue => _dic[_avl.MaxKey];
    /// <summary>格納されているキーと値のペアの数を取得します</summary>
    public int Count => _avl.Count;

    /// <summary>指定されたキーがマップに存在するかどうかを確認します</summary>
    /// <param name="key">確認するキー</param>
    /// <returns>キーが存在する場合はtrue、それ以外はfalse</returns>
    public bool ContainsKey(TKey key) => _avl.Contains(key);

    /// <summary>指定されたキーと値のペアをマップに追加します</summary>
    /// <param name="key">追加するキー</param>
    /// <param name="value">追加する値</param>
    public void Add(TKey key, TValue value)
    {
        // Avoid adding duplicate keys which Dictionary doesn't allow directly
        if (!_dic.ContainsKey(key))
        {
            _avl.Add(key);
            _dic.Add(key, value);
        }
        else
        {
            // Optionally, handle the case where the key already exists,
            // e.g., throw an exception or update the value.
            // For now, just update the value to match Dictionary behavior.
            _dic[key] = value;
        }
    }

    /// <summary>既にキーが存在しない場合のみ、指定されたキーと値のペアをマップに追加します</summary>
    /// <param name="key">追加するキー</param>
    /// <param name="value">追加する値</param>
    /// <returns>キーが追加された場合は true、キーが既に存在した場合は false。</returns>
    public bool TryAdd(TKey key, TValue value)
    {
        if (!_dic.ContainsKey(key))
        {
            _avl.Add(key);
            _dic.Add(key, value);
            return true;
        }
        return false;
    }

    /// <summary>指定されたキーに対応する値を取得します</summary>
    /// <param name="key">値を取得するキー</param>
    /// <returns>指定されたキーに対応する値</returns>
    /// <exception cref="KeyNotFoundException">指定されたキーがマップに存在しない場合。</exception>
    public TValue GetValue(TKey key) => _dic[key];

    /// <summary>指定されたキーに対応する値を設定します。キーが存在しない場合は追加します</summary>
    /// <param name="key">設定するキー</param>
    /// <param name="value">設定する値</param>
    public void SetValue(TKey key, TValue value)
    {
        if (!_dic.ContainsKey(key))
        {
            _avl.Add(key); // Keep AVL tree consistent
        }
        _dic[key] = value; // Dictionary handles add or update
    }

    /// <summary>インデクサー - 指定したキーに対応する値を取得または設定します</summary>
    /// <param name="key">アクセスするキー</param>
    /// <returns>指定されたキーに対応する値</returns>
    /// <exception cref="KeyNotFoundException">get 操作でキーが見つからない場合にスローされます</exception>
    public TValue this[TKey key]
    {
        get => _dic[key];
        set
        {
            if (!_dic.ContainsKey(key))
            {
                _avl.Add(key); // Keep AVL tree consistent
            }
            _dic[key] = value; // Dictionary handles add or update
        }
    }

    /// <summary>指定されたキーとそれに対応する値をマップから削除します</summary>
    /// <param name="key">削除するキー</param>
    /// <returns>要素が正常に削除された場合は true。それ以外の場合は false。</returns>
    public bool Remove(TKey key)
    {
        bool avlRemoved = _avl.Remove(key);
        bool dicRemoved = _dic.Remove(key);
        // Both should ideally return the same result if key exists/doesn't exist
        return avlRemoved && dicRemoved;
    }

    /// <summary>指定されたキーより小さい最大のキーを取得します</summary>
    /// <param name="key">基準となるキー</param>
    /// <param name="minKey">取得できない場合に返すデフォルト値</param>
    /// <returns>指定されたキーより小さい最大のキー、または存在しない場合はminKey</returns>
    public TKey FindKeyLessThan(TKey key, TKey minKey) => _avl.FindLessThan(key, minKey);

    /// <summary>指定されたキーより大きい最小のキーを取得します</summary>
    /// <param name="key">基準となるキー</param>
    /// <param name="maxKey">取得できない場合に返すデフォルト値</param>
    /// <returns>指定されたキーより大きい最小のキー、または存在しない場合はmaxKey</returns>
    public TKey FindKeyGreaterThan(TKey key, TKey maxKey) => _avl.FindGreaterThan(key, maxKey);

    /// <summary>
    /// 指定したキー以下の最大のキーを返します。
    /// </summary>
    /// <param name="key">基準となるキー。</param>
    /// <param name="minKeyIfNotFound">キーが見つからない場合に返す値。</param>
    /// <returns>指定したキー以下の最大のキー。そのようなキーが存在しない場合は <paramref name="minKeyIfNotFound"/> を返します。</returns>
    public TKey FindKeyLessThanOrEqual(TKey key, TKey minKeyIfNotFound) => _avl.FindLessThanOrEqual(key, minKeyIfNotFound);

    /// <summary>
    /// 指定したキー以上の最小のキーを返します。
    /// </summary>
    /// <param name="key">基準となるキー。</param>
    /// <param name="maxKeyIfNotFound">キーが見つからない場合に返す値。</param>
    /// <returns>指定したキー以上の最小のキー。そのようなキーが存在しない場合は <paramref name="maxKeyIfNotFound"/> を返します。</returns>
    public TKey FindKeyGreaterThanOrEqual(TKey key, TKey maxKeyIfNotFound) => _avl.FindGreaterThanOrEqual(key, maxKeyIfNotFound);


    /// <summary>キーを昇順に並べたリストを取得します</summary>
    /// <returns>順序付けされたキーのリスト</returns>
    public List<TKey> InOrderKeys() => _avl.ToSortedList();

    /// <summary>キーの昇順に対応する値のリストを取得します</summary>
    /// <returns>キーの昇順に対応する値のリスト</returns>
    public List<TValue> InOrderValues()
    {
        var keys = InOrderKeys();
        var values = new List<TValue>(keys.Count); // Pre-allocate list capacity

        foreach (var key in keys)
        {
            values.Add(_dic[key]);
        }

        return values;
    }

    /// <summary>キーと値のペアをキーの昇順で取得します</summary>
    /// <returns>キーと値のペアのリスト</returns>
    public List<KeyValuePair<TKey, TValue>> InOrderKeyValuePairs()
    {
        var keys = InOrderKeys();
        var pairs = new List<KeyValuePair<TKey, TValue>>(keys.Count); // Pre-allocate list capacity

        foreach (var key in keys)
        {
            pairs.Add(new KeyValuePair<TKey, TValue>(key, _dic[key]));
        }

        return pairs;
    }


    /// <summary>指定されたキーに対応する値を取得します。キーが見つかったかどうかを示す値を返します</summary>
    /// <param name="key">検索するキー</param>
    /// <param name="value">指定されたキーに対応する値。キーが存在しない場合はdefault(TValue)</param>
    /// <returns>マップ内に指定されたキーが存在する場合はtrue、それ以外はfalse</returns>
    public bool TryGetValue(TKey key, out TValue value)
    {
        // Use Dictionary's TryGetValue for efficiency
        return _dic.TryGetValue(key, out value);
    }

    /// <summary>マップからすべての要素を削除します</summary>
    public void Clear()
    {
        _avl.Clear();
        _dic.Clear();
    }
}