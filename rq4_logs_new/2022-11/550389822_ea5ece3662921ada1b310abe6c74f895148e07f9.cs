using System;

namespace Exercism.SwitchStatements
{
  public static class PlayAnalyzer
  {
    public static string AnalyzeOnField(int shirtNum)
    {
      return shirtNum switch
      {
        1 => "goalie",
        2 => "left back",
        3 or 4 => "center back",
        5 => "right back",
        6 or 7 or 8 => "midfielder",
        9 => "left wing",
        10 => "striker",
        11 => "right wing",
        _ => throw new ArgumentOutOfRangeException(shirtNum.ToString())
      };
    }

    public static string AnalyzeOffField(object report)
    {
      return report switch
      {
        int n => $"There are {n} supporters at the match.",
        string s => s,
        Injury i => $"Oh no! {i.GetDescription()} Medics are on the field.",
        // Foul : Incident も兼任している
        Incident i => $"{i.GetDescription()}",
        Manager m when m.Club != null => m.Name + $" ({m.Club})",
        Manager m => m.Name,
        _ => throw new ArgumentException()
      };
    }
  }
}
