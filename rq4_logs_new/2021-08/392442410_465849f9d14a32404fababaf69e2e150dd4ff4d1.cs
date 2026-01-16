using System.Collections.Generic;

namespace BlackJackTeamProject.Models
{
  public class Dealer
  {
    public string Name = "Dealer";
    public float RoundScore { get; set; }
    public Player.PlayerState State { get; set; }
    public List<Card> Hand { get; set; }

    public Dealer(string name)
    {
      RoundScore = 0;
      State = Player.PlayerState.InactiveTurn;
    }
  }
}