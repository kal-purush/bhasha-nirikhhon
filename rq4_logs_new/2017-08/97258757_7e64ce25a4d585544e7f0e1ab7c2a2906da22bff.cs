using UnityEngine;

public class RandomSound : MonoBehaviour {
    public AudioClip[] audioClips = new AudioClip[0];
    public float volume;

    // Creates audio sources as children to Player
    private void Start() {
        AudioSource audioSource = new AudioSource();
        GameObject child = new GameObject("Music Player");
        int randomNum = Random.Range(0, audioClips.Length);

        child.transform.parent = gameObject.transform;
        audioSource = child.AddComponent<AudioSource>();
        audioSource.volume = volume;
        audioSource.PlayOneShot(audioClips[randomNum]);
    }
}