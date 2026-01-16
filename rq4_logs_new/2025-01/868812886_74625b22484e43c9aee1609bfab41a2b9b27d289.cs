using UnityEngine;
using DG.Tweening; // Import DoTween namespace

public class CollectableIdleAnimation : MonoBehaviour
{
    [Header("Idle Animation Settings")]
    public float rotationSpeed = 45f; // Degrees per second
    public float scaleDuration = 1.0f; // Duration of the scale animation
    public float scaleAmount = 1.1f; // Scale multiplier

    private Vector3 originalScale;

    private void Start()
    {
        // Store the original scale
        originalScale = transform.localScale;

        // Start the idle animation
        StartIdleAnimation();
    }

    private void StartIdleAnimation()
    {
        // Apply subtle rotation animation
        transform.DORotate(Vector3.up * 360, rotationSpeed, RotateMode.LocalAxisAdd)
            .SetEase(Ease.Linear)
            .SetLoops(-1, LoopType.Incremental); // Infinite looping rotation

        // Apply subtle scaling animation
        transform.DOScale(originalScale * scaleAmount, scaleDuration)
            .SetEase(Ease.InOutSine)
            .SetLoops(-1, LoopType.Yoyo); // Infinite looping scale up and down
    }
}