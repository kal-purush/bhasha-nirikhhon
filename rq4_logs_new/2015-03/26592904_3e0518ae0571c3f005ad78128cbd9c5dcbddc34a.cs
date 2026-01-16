using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using Wardraft.Test;

namespace Wardraft.Game {

  public class GameController : MonoBehaviour {
  
    public string MapName;
    public ResourceLoader RL;
    public MapController MC;
  
    Game game;
  
    // MAIN FUNCTION TO START
    void Start () {
      game = new Game();
      checkDependencies();
    }
    
    void Update () {
      if (game.state == Enums.GameState.Unstarted) {
        if (RL.done) {
          startGame();
        }
      }
    }
  
    void startGame () {
      Debug.Log("Starting Game...");
      //TODO: pass in user data and game seed from prior scene
      List<User> users = TestingData.users;
      int seed = 1234567890;
      game.LoadGame(MapName, users, seed);
      MC.LoadMap();
    }
    
    void checkDependencies () {
      if (RL == null) Debug.LogError("Please set ResourceLoader in GameController");
      if (MC == null) Debug.LogError("Please set MapController in GameController");
    }
  
  }

}