using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using DG.Tweening;

public class DoorMove : MonoBehaviour
{
    [SerializeField] private Transform _startPosition;
    [SerializeField] private Transform _endPosition;
    private AudioSource _source;
    void Start()
    {
        _source = GetComponent<AudioSource>();
        transform.DOMove(_endPosition.position, 10f);
    }

    public void GoStartPositionDoor()
    {
        transform.DOMove(_startPosition.position, 2f);
        Debug.Log("Dooor");
    }

    public void PlayCloseSound()
    {
        _source.Play();
    }
}