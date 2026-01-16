using UnityEngine;
using Unity.Cinemachine;
using UnityEngine.SceneManagement;

public class CameraReassign : MonoBehaviour
{
    private CinemachineCamera cinemachineCamera;

    private void Start()
    {
        cinemachineCamera = GetComponent<CinemachineCamera>();
        AssignPlayer();
    }

    private void AssignPlayer()
    {
        GameObject player = GameObject.FindGameObjectWithTag("Player");
        if (player != null)
        {
            cinemachineCamera.Follow = player.transform;
        }
    }

    private void OnEnable()
    {
        SceneManager.sceneLoaded += OnSceneLoaded;
    }

    private void OnDisable()
    {
        SceneManager.sceneLoaded -= OnSceneLoaded;
    }

    private void OnSceneLoaded(Scene scene, LoadSceneMode mode)
    {
        AssignPlayer();
    }
}