using System;
using System.Collections.Generic;
using System.Linq;

namespace Lists
{
  public static class Languages
  {
    private static List<string> _languages =
      new List<string> { "C#", "Clojure", "Elm" };
    public static List<string> NewList() => new List<string> {};
    public static List<string> GetExistingLanguages() => _languages;
    public static List<string> AddLanguage(List<string> languages, string language)
    {
      // .Add() vs .Append()
      return languages.Append(language).ToList();
    }

    public static int CountLanguages(List<string> languages) => languages.Count;
    public static bool HasLanguage(List<string> languages, string language)
    {
      return languages.Contains(language);
    }

    public static List<string> ReverseList(List<string> languages)
    {
      // Reverse() と Reverse<string>() は異なる
      return languages.Reverse<string>().ToList();
    }

    public static bool IsExciting(List<string> languages)
    {
      var idx = languages.IndexOf("C#");
      return (idx == 0 || (idx == 1 && languages.Count <= 3));
    }

    public static List<string> RemoveLanguage(List<string> languages, string language)
    {
      var idx = languages.IndexOf(language);
      if (0<idx)
      {
        languages.RemoveAt(idx);
      }
      return languages;
    }

    public static bool IsUnique(List<string> languages)
    {
      var hs = new HashSet<string>(languages);
      return hs.Count == languages.Count;
    }
  }
}
