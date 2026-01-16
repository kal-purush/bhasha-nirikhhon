using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EndReached : MonoBehaviour
{
    [SerializeField] private ParticleSystem particle;
    [SerializeField] private Transform spawn;
    [SerializeField] private GameObject player;
    [SerializeField] private Rigidbody rbPlayer;

    private void OnTriggerEnter(Collider other)
    {
        if(other.gameObject.CompareTag("Player")){
            particle.gameObject.SetActive(true);
            Invoke("resetPlayer", 1f);
            Invoke("disableParticle", 1f);
        }
    }

    public void resetPlayer()
    {
        player.transform.position = spawn.position;
    }
    public void disableParticle()
    {
        particle.gameObject.SetActive(false);
    }
    public void tracked()
    {
        //rbPlayer.isKinematic = false;
    }
    public void notTracked()
    {
        //rbPlayer.isKinematic = true;
    }

}