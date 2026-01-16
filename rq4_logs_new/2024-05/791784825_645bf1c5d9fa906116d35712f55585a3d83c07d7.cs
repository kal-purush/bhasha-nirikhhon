using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour
{
    public Transform spawnPosition;

    private void Awake()
    {
        if (PlayerStats.Instance.isDead)
        {
            PlayerStats.Instance.isDead = false;
            PlayerStats.Instance.gameObject.transform.position = spawnPosition.transform.position;
            PlayerStats.Instance.gameObject.SetActive(true);
        }
    }
}