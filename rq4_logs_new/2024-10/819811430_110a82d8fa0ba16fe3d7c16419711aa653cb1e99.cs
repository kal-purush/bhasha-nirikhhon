using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Player_Trigger : MonoBehaviour
{
    public enum Trigger_Mode
    {
        Rotation_When_Trigger,
        Stop_When_Trigger
    }
    [SerializeField] private float RotationDuration;
    [SerializeField] private float RotationAngle;
    [SerializeField] private float RotationDelay;
    [SerializeField] private float StoppingDelay = 1.5f;
    [SerializeField] private bool KeepMoving = false;
    [SerializeField] private Trigger_Mode trigger_Mode;

    void OnTriggerEnter(Collider other)
    {
        if (other.CompareTag("Player"))
        {
            switch (trigger_Mode)
            {
                case Trigger_Mode.Rotation_When_Trigger:
                    Player_Movement.instance.Rotation_Player(other.gameObject, RotationAngle, RotationDelay, KeepMoving);
                    break;
                case Trigger_Mode.Stop_When_Trigger:
                    Player_Movement.instance.Stopping_Player(other.gameObject, StoppingDelay, KeepMoving);
                    break;

            }
            gameObject.SetActive(false);
        }
    }
}