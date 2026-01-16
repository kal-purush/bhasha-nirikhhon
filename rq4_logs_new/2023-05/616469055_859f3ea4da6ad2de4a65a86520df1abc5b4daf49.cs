using System;
using System.Collections.Generic;
using UnityEngine;

namespace Runtime.AI
{
    [RequireComponent(typeof(AIController))]
    public class AISound : MonoBehaviour
    {
        [SerializeField] private AudioSource[] chaseSound;
        [SerializeField] private AudioSource[] huntSound;
        [SerializeField] private AudioSource[] searchingSound;
        [SerializeField] private AudioSource[] roamingSound;
        
        private AIController _controller;
        private List<AudioSource> _currentPlaying = new();
        private void Awake()
        {
            _controller = GetComponent<AIController>();
        }

        private void Start()
        {
            _controller.onStateChange.AddListener(HandleStateChange);
        }

        private void HandleStateChange(AIState state)
        {
            switch (state)
            {
                case AIState.Chasing:
                    Play(chaseSound);
                    break;
                case AIState.Searching:
                    Play(searchingSound);
                    break;
                case AIState.Roaming:
                    Play(roamingSound);
                    break;
                case AIState.ForcedHunt:
                    Play(huntSound);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(state), state, null);
            }
        }

        private void Play(params AudioSource[] source)
        {
            _currentPlaying.ForEach(audioSource =>
            {
                audioSource.Stop();
            });
            
            _currentPlaying.Clear();

            foreach (var audioSource in source)
            {
                _currentPlaying.Add(audioSource);
                audioSource.Play();
            }
        }
    }
}