using System.Collections;
using UnityEngine;

namespace _Scripts
{
    public class PlaySoundOnFinish : MonoBehaviour
    {
        [SerializeField] private AudioSource other;
        private AudioSource self;

        void Start()
        {
            self = GetComponent<AudioSource>();
            StartCoroutine(WaitUntilPlayed());
        }

        IEnumerator WaitUntilPlayed()
        {
            yield return new WaitUntil(() => self.isPlaying == false);
            other.Play();
        }
    }
}