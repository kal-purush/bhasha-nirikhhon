using UnityEngine.SceneManagement;
using UnityEngine.UI;
using UnityEngine;

public class MenuInicio : MonoBehaviour
{
    public Button btnPlay;
    public Button btnCreditos;
    public Button btnOptions;
    public Button btnExits;
    // Start is called before the first frame update
    void Start()
    {
        btnPlay.onClick.AddListener(LoadLevel1);
        btnCreditos.onClick.AddListener(LoadCreditos);
        btnOptions.onClick.AddListener(LoadOptions);
        btnExits.onClick.AddListener(LoadExits);
    }
    void LoadLevel1()
    {
        SceneManager.LoadScene("Level1");
    }
    void LoadCreditos()
    {
        SceneManager.LoadScene("Creditos");
    }
    void LoadOptions()
    {
        SceneManager.LoadScene("Controles");
    }
    void LoadExits()
    {
        Application.Quit();
    }
    // Update is called once per frame
    void Update()
    {
        
    }
}