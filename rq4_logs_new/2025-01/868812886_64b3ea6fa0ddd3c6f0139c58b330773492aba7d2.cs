using UnityEngine;
using DG.Tweening; // Import DoTween namespace

public class DoorController : MonoBehaviour
{
    public Transform doorLeft;
    public Transform doorRight;
    public float openDistance = 5f; // Distance to detect player
    public float closeDistance = 5f; // Distance to play beep sound if locked
    public Vector3 leftOpenPosition;
    public Vector3 rightOpenPosition;
    public Vector3 leftClosedPosition;
    public Vector3 rightClosedPosition;
    public float animationDuration = 2f; // Animation duration in seconds
    public AudioSource audioSource;
    public AudioClip openSound;
    public AudioClip beepSound; // Sound to play when too close and locked
    public Material doorMaterial;
    public string lockedColorHex = "#FF8C8C"; // Hex color for locked (red)
    public string unlockedColorHex = "#8CFF90"; // Hex color for unlocked (green)

    private Transform player;
    private bool isDoorOpen = false;
    private bool hasKey = false; // Simulates whether the player has the key

    void Start()
    {
        player = GameObject.FindGameObjectWithTag("Player").transform;
        if (doorLeft == null || doorRight == null)
        {
            Debug.LogError("Door parts not assigned in the inspector");
        }
        if (audioSource == null)
        {
            Debug.LogError("AudioSource not assigned in the inspector");
        }
        if (doorMaterial != null)
        {
            UpdateDoorMaterial();
        }
    }

    void Update()
    {
        if (player != null)
        {
            float distanceToPlayer = Vector3.Distance(player.position, transform.position);

            if (distanceToPlayer <= openDistance && !isDoorOpen && hasKey)
            {
                OpenDoor();
            }
            else if (distanceToPlayer > openDistance && isDoorOpen)
            {
                CloseDoor();
            }
            if (distanceToPlayer <= closeDistance && !hasKey)
            {
                Debug.Log("play sound!");
                PlayBeepSound();
            }
        }

        if(!hasKey){
            SetHasKey(AppHelper.hasKey);
        }
    }

    void OpenDoor()
    {
        isDoorOpen = true;
        doorLeft.DOLocalMove(leftOpenPosition, animationDuration);
        doorRight.DOLocalMove(rightOpenPosition, animationDuration);
        if (audioSource != null && openSound != null)
        {
            audioSource.PlayOneShot(openSound);
        }
    }

    void CloseDoor()
    {
        isDoorOpen = false;
        doorLeft.DOLocalMove(leftClosedPosition, animationDuration);
        doorRight.DOLocalMove(rightClosedPosition, animationDuration);
        if (audioSource != null && openSound != null)
        {
            AudioClip reversedClip = ReverseAudioClip(openSound);
            audioSource.PlayOneShot(reversedClip);
        }
    }

    void PlayBeepSound()
    {
        if (audioSource != null && beepSound != null && !audioSource.isPlaying)
        {
            audioSource.PlayOneShot(beepSound);
        }
    }

    AudioClip ReverseAudioClip(AudioClip clip)
    {
        float[] samples = new float[clip.samples * clip.channels];
        clip.GetData(samples, 0);

        // Reverse the samples
        System.Array.Reverse(samples);

        AudioClip reversedClip = AudioClip.Create(clip.name + "_Reversed", clip.samples, clip.channels, clip.frequency, false);
        reversedClip.SetData(samples, 0);
        return reversedClip;
    }

    public void SetHasKey(bool keyStatus)
    {
        hasKey = keyStatus;
        UpdateDoorMaterial();
    }

    void UpdateDoorMaterial()
    {
        if (doorMaterial != null)
        {
            Color color;
            if (ColorUtility.TryParseHtmlString(hasKey ? unlockedColorHex : lockedColorHex, out color))
            {
                doorMaterial.color = color;
            }
            else
            {
                Debug.LogError("Invalid hex color code provided.");
            }
        }
    }
}