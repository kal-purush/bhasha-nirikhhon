using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerCollision : MonoBehaviour
{
    public GameObject GameController;
    private GameController _gameControllerScript;

    public void Start()
    {
        _gameControllerScript = GameController.GetComponent<GameController>();
    }
    public void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "Enemy")
        {
            _gameControllerScript.GameOver();
            Destroy(gameObject);
        }
    }
}