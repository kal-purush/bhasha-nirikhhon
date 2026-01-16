using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
public class EasyButton : MonoBehaviour
{
    // Start is called before the first frame update
    public void OnButtonPress()
    {
        SceneManager.LoadScene("_Scene_0");
    }
}