using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SpawnScript : MonoBehaviour
{
    public GameObject[] enemies;
    private float _rightSide;
    private float _bottomSide;
    private float _leftSide;
    private float _topSide;

    void Start()
    {
        _rightSide = transform.position.x + transform.localScale.x / 2;
        _bottomSide = transform.position.z - transform.localScale.z / 2;
        _leftSide = transform.position.x - transform.localScale.x / 2;
        _topSide = transform.position.z + transform.localScale.z / 2;
    }

    public void Spawn(int index)
    {
        Instantiate(enemies[index], new Vector3(Random.Range(_leftSide, _rightSide), 0, Random.Range(_bottomSide, _topSide)), enemies[index].transform.rotation);
    }
}