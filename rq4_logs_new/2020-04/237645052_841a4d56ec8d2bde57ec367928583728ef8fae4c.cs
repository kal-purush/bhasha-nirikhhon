using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayAnimation : MonoBehaviour
{
    ParticleSystem playParticle;
    [SerializeField]
    int playOnce = 0;

    private void Awake()
    {
        playParticle = this.GetComponent<ParticleSystem>();
    }
    // Start is called before the first frame update
    void Start()
    {
    }

    // Update is called once per frame
    void Update()
    {
        if(playOnce == 0 && this.enabled == true)
        {
            playParticleSystem();
            playOnce = 1;
        }
    }
    /// <summary>
    /// Play current ParticleSystem Object reference
    /// </summary>
    private void playParticleSystem()
    {
        playParticle.Play();
    }
    /// <summary>
    /// Stop current ParticleSystem object reference
    /// </summary>
    private void stopParticleSystem()
    {
        playParticle.Stop();
    }

    public void resetPlayParticleSystem()
    {
        playOnce = 0;
    }
}