using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MainController : MonoBehaviour
{
    static bool isGaming;
    public static float screenWidth;
    public static float screenHeight;
    public Player _player;
    public GameObject _moonCake;
    private Player player;
    private GameObject moonCake;
    private bool isPause;
    private float moonCakeRadius;

    void Start()
    {
        isGaming = false;
        this.isPause = false;
        screenHeight = Camera.main.orthographicSize * 2;
        screenWidth = screenHeight * Screen.width / Screen.height;
        moonCakeRadius = _moonCake.GetComponent<Collider2D>().bounds.size.y / 2;

        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void StartGame()
    {
        isGaming = true;
        this.player = Instantiate(_player);
        if (isGaming)
        {
            Invoke("Spawn", 2);
        }
    }

    public void Spawn()
    {
        Debug.Log("ok");
        Vector3 position = new Vector3(UnityEngine.Random.Range(-screenWidth / 2 + moonCakeRadius, screenWidth / 2 - moonCakeRadius),
            UnityEngine.Random.Range(-screenHeight / 2 + moonCakeRadius, screenHeight / 2 - moonCakeRadius), 0);
        if (Physics2D.OverlapCircle(position, moonCakeRadius) == null)
        {
            _moonCake.transform.position = position;
            Instantiate(_moonCake, _moonCake.transform);
        }
    }
}