using System;
using UnityEngine;
using Zenject;

public class RaceManager : MonoBehaviour
{
    public float ElapsedTime { get; private set; }

    public GameState CurrentState { get; private set; }
    public Action<GameState, GameState> OnGameStateUpdated;

    private SceneLoader _sceneLoader;
    private PauseService _pauseService;

    [Inject]
    private void Construct(SceneLoader sceneLoader, PauseService pauseService)
    {
        _sceneLoader = sceneLoader;
        _pauseService = pauseService;

        _sceneLoader.OnGameLevelChanged += OnGameLevelChanged;
        UpdateGameState(GameState.Pregame);
    }

    private void Update()
    {
        if (CurrentState == GameState.Running)
        {
            ElapsedTime += Time.deltaTime;
        }
    }

    private void OnDestroy()
    {
        _sceneLoader.OnGameLevelChanged -= OnGameLevelChanged;
    }

    private void OnGameLevelChanged(Scene scene)
    {
        UpdateGameState(GameState.Pregame);
    }

    public void RunGame()
    {
        UpdateGameState(GameState.Running);
    }

    public void FinishGame()
    {
        UpdateGameState(GameState.Finished);
    }

    public void PauseGame()
    {
        UpdateGameState(GameState.Paused);
    }

    public void UpdateGameState(GameState newState)
    {
        var prevState = CurrentState;
        CurrentState = newState;
        switch (newState)
        {
            case GameState.Pregame:
                HandlePregameState();
                break;
            case GameState.Running:
                HandleRunningState();
                break;
            case GameState.Paused:
                HandlePauseState();
                break;
            case GameState.Finished:
                HandleFinishedState();
                break;
        }
        OnGameStateUpdated?.Invoke(prevState, newState);
    }

    private void HandleRunningState()
    {
        _pauseService.Resume();
    }

    private void HandleFinishedState()
    {
        _pauseService.SetPause();
    }

    private void HandlePauseState()
    {
        _pauseService.SetPause();
    }

    private void HandlePregameState()
    {
        _pauseService.SetPause();
        ElapsedTime = 0;
    }
}