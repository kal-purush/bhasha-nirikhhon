using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Weapon : MonoBehaviour
{
    public Transform shoot;
    [SerializeField] private byte type = 0;
    [SerializeField] private float delay = 0.1f;
    [SerializeField] [Range(0, 45)] private float spreadAngle = 15f;
    [SerializeField] private int numsOfShots = 1;

    public bool IsFiring { get; set; } = false;

    private float timer = 0f;
    private readonly float duration = 0.25f;
    private void Update()
    {
        timer -= Time.deltaTime;

        if (timer < 0)
        {
            IsFiring = false;
        }
        print(IsFiring);
        print(timer);
    }

    public void Shoot() 
    {
        timer = duration;
        StartCoroutine(Fire());
    }

    private IEnumerator Fire() 
    {
        Vector2 variation = Vector2.zero;
        for (int i = 0; i < numsOfShots; i++)
        {
            GameManager.Instance.Shoot(shoot, type, variation, GameManager.Instance.Entities[GameManager.Instance.ThisPlayer]);
            variation.x = Random.Range(-spreadAngle, spreadAngle + 1);
            variation.y = Random.Range(-spreadAngle, spreadAngle + 1);
            yield return new WaitForSeconds(delay);
        }
    }
}