using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NewCameraController : MonoBehaviour
{
    [SerializeField] private GameObject character;

    private void Update()
    {
        if (character == null) return;
        gameObject.transform.position = character.transform.position -5 * Vector3.forward;
    }

    private void Start()
    {
        StartCoroutine(LateStart());
    }

    private IEnumerator LateStart()
    {
        yield return null;
        character = GameObject.FindGameObjectWithTag("Player");
    }
}