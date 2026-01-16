using System.Collections;
using System.Collections.Generic;
using Assets.GameSystem.Constant.Enum;
using GameSystem.Service;
using UnityEngine;
using UnityEngine.SceneManagement;

public class MainMenuManager : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void ToSettings()
    {
        SceneService.Instance.ChangeScene(SceneEnum.Settings);
    }
}