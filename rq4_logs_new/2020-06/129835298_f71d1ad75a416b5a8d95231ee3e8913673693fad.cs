using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using SNDL;

namespace SM
{
  public class SMUIGateNavigationGuide : MonoBehaviour
  {
    public SMLevelGate gate;
    private Transform player;
    private SMGame game;
    private Camera gameCamera;
    // Start is called before the first frame update
    void Start()
    {
      game = Game.GetGame<Game>() as SMGame;
      gameCamera = game.view.cam;
      player = game.controller.currentPawn.transform;
    }

    // Update is called once per frame
    void Update()
    {
      if (player != null && gameCamera != null)
      {
        this.transform.position = player.position;
        Vector3 screenPoint = gameCamera.WorldToScreenPoint(gate.transform.position);

        Debug.Log("screen point " + screenPoint);
      }
      else
      {
        Debug.Log("no player found");
      }
    }
  }
}