using System;
using System.Collections.Generic;
using System.Linq;

namespace Exercism.Dictionaries
{
  public static class DialingCodes
  {
    public static Dictionary<int, string> GetEmptyDictionary() => new Dictionary<int, string>();
    public static Dictionary<int, string> GetExistingDictionary()
    {
      return new Dictionary<int, string>
      {
        {1, "United States of America" },
        {55, "Brazil" },
        {91, "India" }
      };
    }
    public static Dictionary<int, string> AddCountryToEmptyDictionary(int countryCode, string countryName)
    {
      return new Dictionary<int, string>
      {
        {countryCode, countryName }
      };
    }
    public static Dictionary<int, string> AddCountryToExistingDictionary(
        Dictionary<int, string> existingDictionary, int countryCode, string countryName)
    {
      existingDictionary.Add(countryCode, countryName);
      return existingDictionary;
    }
    public static string GetCountryNameFromDictionary(
        Dictionary<int, string> existingDictionary, int countryCode)
    {
      // string のデフォルト値が null
      return existingDictionary.GetValueOrDefault(countryCode) ?? String.Empty;
    }
    public static bool CheckCodeExists(Dictionary<int, string> existingDictionary, int countryCode)
    {
      return existingDictionary.ContainsKey(countryCode);
    }
    public static Dictionary<int, string> UpdateDictionary(
        Dictionary<int, string> existingDictionary, int countryCode, string countryName)
    {
      if (CheckCodeExists(existingDictionary, countryCode))
      {
        existingDictionary[countryCode] = countryName;
      }
      return existingDictionary;
    }
    public static Dictionary<int, string> RemoveCountryFromDictionary(
        Dictionary<int, string> existingDictionary, int countryCode)
    {
      existingDictionary.Remove(countryCode);  // キーがなくてもエラーにはならない
      return existingDictionary;
    }
    public static string FindLongestCountryName(Dictionary<int, string> existingDictionary)
    {
      return existingDictionary.Values
        .OrderByDescending(name => name.Length)
        .FirstOrDefault(String.Empty);
    }
  }
}
