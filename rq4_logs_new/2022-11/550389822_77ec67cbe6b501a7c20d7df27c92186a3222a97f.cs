using System.Text;

namespace Exercism.Chars
{
  public static class Identifier
  {
    static bool IsGreekCase(char c) => 'α' <= c && c <= 'ω';
    public static string Clean(string identifier)
    {
      var sb = new StringBuilder();
      bool isAfterDash = false;

      foreach (char c in identifier)
      {
        sb.Append(c switch
        {
          _ when isAfterDash => char.ToUpperInvariant(c),  // 共通のカルチャインフォ
          _ when IsGreekCase(c) => string.Empty,
          _ when char.IsWhiteSpace(c) => '_',
          _ when char.IsControl(c) => "CTRL",
          _ when char.IsLetter(c) => c,
          _ => string.Empty,
        });

        isAfterDash = c.Equals('-');
      }

      return sb.ToString();
    }
  }
}
