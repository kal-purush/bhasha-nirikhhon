using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ParticleBells : MonoBehaviour
{
    private ParticleSystem.Particle[] allParts;

    public float switchFrequency;

    private ParticleSystem myPartSystem;

    private float t;

    private Vector2 particlePosition;

    public Transform player;

    public float distanceThreshold;

    public AK.Wwise.Event PlayBellEvent;
    
    // Start is called before the first frame update
    void Start()
    {

        myPartSystem = GetComponent<ParticleSystem>();
        allParts = new ParticleSystem.Particle[myPartSystem.main.maxParticles];
        myPartSystem.GetParticles(allParts);
        ParticleSystem.Particle chosenParticle = allParts[Random.Range(0, allParts.Length)];
        particlePosition = new Vector2(chosenParticle.position.x, chosenParticle.position.z);
        Debug.Log("New Particle Position: " + particlePosition);
        
    }

    // Update is called once per frame
    void Update()
    {
        t += Time.deltaTime;
        if (t >= switchFrequency)
        {
            t = 0f;
            
            myPartSystem.GetParticles(allParts);
            ParticleSystem.Particle chosenParticle = allParts[Random.Range(0, allParts.Length)];
            particlePosition = new Vector2(chosenParticle.position.x, chosenParticle.position.z);
            Debug.Log("New Particle Position: " + particlePosition);
        }

        Vector2 playerPos = new Vector2(player.position.x, player.position.z);

        float dist = Vector2.Distance(playerPos, particlePosition);

        if (dist < distanceThreshold)
        {
            // play sound
            PlayBellEvent.Post(gameObject);
            Debug.Log("Playing Bell Sound!");
            t = switchFrequency + 1f;
        }

    }
    
    
}