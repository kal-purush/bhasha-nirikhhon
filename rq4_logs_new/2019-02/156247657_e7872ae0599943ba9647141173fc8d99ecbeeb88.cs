using System;
using System.Collections;
using System.Collections.Generic;
using MastersOfTempest.Networking;
using UnityEngine;

namespace MastersOfTempest.PlayerControls.Spellcasting
{
    [RequireComponent(typeof(AudioSource))]
    public class SpellDependantSoundEffect : NetworkBehaviour
    {
        private AudioSource audioSource;

        private SpellcastingController spellcastingController;

        protected override void StartServer()
        {
            spellcastingController = FindObjectOfType<SpellcastingController>();
            if (spellcastingController == null)
            {
                throw new InvalidOperationException($"{nameof(spellcastingController)} is not specified!");
            }
            spellcastingController.SpellCasted += OnSpellCasted;
        }

        protected override void StartClient()
        {
            audioSource = GetComponent<AudioSource>();
        }

        private void PlaySound()
        {
            if(serverObject.onServer)
            {
                SendToAllClients(new byte[1], Facepunch.Steamworks.Networking.SendType.Reliable);
            }
            else
            {
                if(!audioSource.isPlaying)
                {
                    audioSource.Play();
                }
            }
        }

        protected override void OnClientReceivedMessageRaw(byte[] data, ulong steamID)
        {
            PlaySound();
        }

        private void OnSpellCasted(object sender, EventArgs args)
        {
            PlaySound();
        }
    }
}