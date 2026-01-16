using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Stats : MonoBehaviour
{

    //Char stats
    public float health = 10;
    public float speed = 10.0f;
    public float luck = 0.0f;

    //Shoot stats
    public Vector3 shootOffset = new Vector3(0, 0, 0);
    public float shotStartVerticalDegree = 45;
    public float shotSpeed = 1.0f;
    public float fireRate = 1.0f;
    public List<Classes.ShotMode> possibleShotMode;

    public Classes.ShotMode _randomShotType { get { return possibleShotMode[Mathf.RoundToInt(Random.value * possibleShotMode.Count) - 1]; } }

    public void Awake()
    {
        possibleShotMode = new List<Classes.ShotMode>();
        possibleShotMode.Add(Classes.ShotMode.Bomb);
    }

    public void TakeDamage(int damage, Vector3 pushDirection, int force)
    {
        health -= damage;
        if (health <= 0)
        {

        }
        if (gameObject.tag == "Enemy")
        {
            gameObject.GetComponent<Enemy>().TakeDamage(pushDirection, force);
        }
    }
}