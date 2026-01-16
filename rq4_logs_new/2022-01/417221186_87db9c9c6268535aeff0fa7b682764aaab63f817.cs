using System.Collections;
using UnityEngine;

public class EnemySoundFX : MonoBehaviour
{
    public EnemyController ec;
    public AudioSource audio;

    public float walkRate = 0.6f;
    private float walkCooldown = 0f;

    // Start is called before the first frame update
    void Awake()
    {
        ec = GetComponent<EnemyController>();
        audio = GetComponent<AudioSource>();
    }

    private void Update()
    {
        walkCooldown -= Time.deltaTime;
        if (ec.move.magnitude > 0f && walkCooldown <= 0 )
        {
            audio.volume = Random.Range(0.1f, 0.2f);
            audio.pitch = Random.Range(0.8f, 1.1f);
            audio.Play();
            walkCooldown = walkRate;
        }
    }


}