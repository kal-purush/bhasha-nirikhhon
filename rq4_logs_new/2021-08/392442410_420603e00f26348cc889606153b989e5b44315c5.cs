//create api controller

using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using BlackJackTeamProject.Models;
namespace BlackJackTeamProject.Controllers
{
  public class GameController : Controller
  {
   
    public ActionResult Index(){
      return View();
    }

    // GET: Home  
    [HttpGet]
    [Route("/showhand/{name}")]
    public List<Card> ShowHand(string name)
    {
      List<Player> players = Game.Players;
      Player player = players.Find(x => x.Name == name);
      List<Card> cards = player.Hand;
      return cards;
    }

    [HttpPost]
    [Route("/hit/{name}")]
    public void hit(string name)
    {
        
    }

    [HttpPost]
    [Route("/hold/{name}")]
    public void hold(string name)
    {

    }

    [HttpPost]
    [Route("/makeplayer/{name}")]
    public void MakePlayer(string name)
    {
      Player player = new Player(name);
      Game.Players.Add(player);
    }
  }
}

