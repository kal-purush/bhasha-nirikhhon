using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CubeBlue : MonoBehaviour, PlayerInterface
{
    public static bool isBluePressed = false; // Biến kiểm tra xem người chơi có nhấn đúng Blue hay không
    Animator animator;
    private void Start()
    {
        animator = transform.parent.GetComponent<Animator>();
    }
    private void Update()
    {
        AnimaButton();
    }
    public void Interact()
    {
        // Khi người chơi nhấn vào cube xanh
        if (GameLaser.Instance.CheckOrder("Blue"))
        {
            isBluePressed = true;
            Debug.Log("Blue Cube pressed correctly!");
        }
        else
        {
            GameLaser.Instance.ResetOrder();
            Debug.Log("Wrong order! Resetting.");
        }
    }
    public void AnimaButton()
    {
        if (isBluePressed == true)
        {
            animator.SetBool("button", true);
        }
        else if (GameLaser.Instance.currentStep == 0)
        {
            animator.SetBool("button", false);
        }
    }
}