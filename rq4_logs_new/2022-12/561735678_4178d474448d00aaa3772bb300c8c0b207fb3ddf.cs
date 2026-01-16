using System;
using UnityEngine;
using UnityEngine.SceneManagement;

public class PlayerOnDestroy : MonoBehaviour
{
    private CreatureBody _creatureBody;
    
    private void Start()
    {
        _creatureBody = GetComponent<CreatureBody>();
    }

    private void OnDestroy()
    {
        if (_creatureBody.Health <= 0)
        {
            // save stats
            // if hp <= 0 -> meta game
            // else -> next level + save stats
            SceneManager.LoadScene("MetaGame");
        }
    }
}