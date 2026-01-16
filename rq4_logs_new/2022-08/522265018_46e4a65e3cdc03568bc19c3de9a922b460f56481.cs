using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class NextLevel : MonoBehaviour
{
    [SerializeField] private string _name;
   public void GoNextLevel()
   {
        SceneManager.LoadScene(_name);
    }
}