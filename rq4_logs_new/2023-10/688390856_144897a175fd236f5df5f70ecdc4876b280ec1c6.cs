using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Spooky
{
    [System.Serializable]
    public struct SequenceIndex
    {
        public SequenceInteractionComponent component;
        public AudioClip oneShot;
    }
    public class SequenceInteraction : MonoBehaviour
    {
        [SerializeField]
        SequenceIndex[] sequence;
        int currentIndex = 0;

        private void Start()
        {
            for (int i = 0; i < sequence.Length; i++)
            {
                sequence[i].component.controller = this;
            }
            if (sequence.Length > 0)
            {
                sequence[0].component.GetComponent<AudioSource>().volume = 1;
                sequence[0].component.isEnabled = true;
                print(sequence[0].component.name);
            }
        }

        public void increaseIndex()
        {
            sequence[currentIndex].component.isEnabled = false;
            sequence[currentIndex].component.GetComponent<AudioSource>().volume = 0.1f; //fippla med
            if(sequence[currentIndex].oneShot)
            {
                AudioSource source = gameObject.AddComponent<AudioSource>();
                source.clip = sequence[currentIndex].oneShot;
                source.Play();
                print("playOneshot");
            }
            currentIndex++;
            if (currentIndex < sequence.Length) 
            { 
                sequence[currentIndex].component.isEnabled = true;
                sequence[currentIndex].component.GetComponent<AudioSource>().volume = 1;
                if(sequence[currentIndex].component.componentToEnable)
                {
                    sequence[currentIndex].component.componentToEnable.
                }
                print(sequence[currentIndex].component.name);
            }
        }

    }
}