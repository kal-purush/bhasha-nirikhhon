using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class VaultScript : MonoBehaviour
{
    [SerializeField] public AudioClip pickupSoundClip;
    private AudioSource audioSource;
    public GemCollect gemCollect;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    void OnTriggerEnter(Collider other)
    {
        if (other.CompareTag("Player") && gemCollect.gemCnt == 3)
        {
            Debug.Log("tyt");
            if (audioSource != null)
            {
                audioSource.clip = pickupSoundClip;
                audioSource.volume = 10f;
                audioSource.loop = false;  // ��������� ������������
                audioSource.Play();
            }
            else
            {
                Debug.LogWarning("AudioSource is null, please ensure it's attached to the GameObject.");
            }

            // ���������� �������� �� 1 � �������������� �������
            GemCollect.finishCollect = true;
        }
    }
}