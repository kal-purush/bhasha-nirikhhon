using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class PlayGameLoad : MonoBehaviour
{
   void OnEnable()
   {
    SceneManager.LoadScene("NaPuaHoku", LoadSceneMode.Single);
   }
}