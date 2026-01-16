using UnityEngine;

public class GunSound : MonoBehaviour
{
    //������ �������� �������
    public AudioClip[] soundsGun;

    private AudioSource gun;

    public void PlaySoundClip(int index)
    {
        gun = gameObject.GetComponent<AudioSource>();

        gun.clip = soundsGun[index];

        gun.Play();
    }
}