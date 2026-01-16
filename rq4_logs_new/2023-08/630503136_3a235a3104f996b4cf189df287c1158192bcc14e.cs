using System;
using UnityEditor;
using UnityEngine;
using UnityEngine.SceneManagement;
using Zenject;
using Cysharp.Threading.Tasks;


public class App : MonoBehaviour, IInitializable, IDisposable
{
    [SerializeField] private SceneAsset sceneAsset;
    private string _currentScene = String.Empty;
    private Scene _baseScene;
    
    public async void Initialize()
    {
        _baseScene = SceneManager.GetActiveScene();
        SceneManager.sceneLoaded += OnSceneLoaded;
        await LoadScene(sceneAsset.name);
    }

    public void Dispose()
    {
        SceneManager.sceneLoaded -= OnSceneLoaded;
    }

    private void OnSceneLoaded(Scene scene, LoadSceneMode mode)
    {
        SceneManager.SetActiveScene(scene);
    }

    public async UniTask LoadScene(string sceneName)
    {
        if (_currentScene != String.Empty)
        {
            SceneManager.SetActiveScene(_baseScene);
            await SceneManager.UnloadSceneAsync(_currentScene);
        }

        await SceneManager.LoadSceneAsync(sceneName, LoadSceneMode.Additive);
        _currentScene = sceneName;
    }

    public async UniTask ReloadScene()
    {
        await LoadScene(_currentScene);
    }
}