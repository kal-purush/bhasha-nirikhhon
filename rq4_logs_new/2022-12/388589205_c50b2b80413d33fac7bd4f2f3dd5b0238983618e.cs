using System;
using System.Collections;
using System.Collections.Generic;


public class LazyDictionary<TKey, TValue> : Dictionary<TKey, TValue>
{
  public System.Func<TKey, TValue> Get;

  public new TValue this[TKey key] => this.Get(key);

  public LazyDictionary(System.Func<TKey, TValue> lookupFunction)
  {

    Get = key =>
    {
      TValue value;
      if (!TryGetValue(key, out value))
      {
        value = lookupFunction(key);
        Add(key, value);
      }
      return value;
    };
  }
}
