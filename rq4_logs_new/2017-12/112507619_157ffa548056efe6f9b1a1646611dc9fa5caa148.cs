using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;

public class RoundController : MonoBehaviour
{

    public GameManager gm;
    // Use this for initialization
    private void Awake()
    {
        DontDestroyOnLoad(gameObject);
    }
    void Start()
    {
        gm = GameManager.Instance;
    }

    // Update is called once per frame
    void Update()
    {
        if(SceneManager.GetActiveScene().name == "Round Transition")
        {
            gm.gameObject.SetActive(false);
        }
        else if(SceneManager.GetActiveScene().name == "Game Over")
        {
            Destroy(gameObject);
        }
        else
        {
            gm.gameObject.SetActive(true);
        }
    }
}