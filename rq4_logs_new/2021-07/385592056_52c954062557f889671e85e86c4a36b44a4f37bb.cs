using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class SceneChange : MonoBehaviour
{

    public int levelnum;

    public void SetScene() {

        SceneManager.LoadScene(levelnum);

    }

}