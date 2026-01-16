using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using UnityEngine;

public class GameStateManager : MonoBehaviour
{
    public static GameStateManager Instance;

    public enum Game_Status
    {
        SprayGameStart,
        SprayGameEnd,
        PickupGameStart,
        PickupGameEnd,
        MatchGameStart,
        MatchGameEnd,

    }
    public enum Player_Status
    {
        PlayerInGame,
        PlayerMoving,
        PlayerRotating,
    }
    public Game_Status game_Status { get; set; } = Game_Status.SprayGameStart;
    public Player_Status player_Status { get; set; } = Player_Status.PlayerInGame;
    public event EventHandler OnGameStateChange;

    void Start()
    {
        if (Instance == null)
        {
            Instance = this;
        }
        game_Status = Game_Status.SprayGameStart;
        Plant_Progress.OnGameStateChange += Handle_OnGameStateChange;

    }
    void Update()
    {
        Player_Status_Fun();
    }
    private void Handle_OnGameStateChange(object sender, EventArgs e)
    {
        Game_Status_Fun();
    }

    private void Game_Status_Fun()
    {
        switch (game_Status)
        {
            case Game_Status.SprayGameStart:

                break;
            case Game_Status.SprayGameEnd:
                SprayGameManager.Instance.AfterSprayGame();
                break;
            case Game_Status.PickupGameStart:

                break;
            case Game_Status.PickupGameEnd:

                break;
            case Game_Status.MatchGameStart:

                break;
            case Game_Status.MatchGameEnd:

                break;
        }
    }
    private void Player_Status_Fun()
    {
        switch (player_Status)
        {
            case Player_Status.PlayerInGame:
                Player_Movement.instance.CanMove(false);
                break;
            case Player_Status.PlayerRotating:
                Player_Movement.instance.CanMove(false);
                break;
            case Player_Status.PlayerMoving:
                Player_Movement.instance.CanMove(true);
                break;
        }
    }



}