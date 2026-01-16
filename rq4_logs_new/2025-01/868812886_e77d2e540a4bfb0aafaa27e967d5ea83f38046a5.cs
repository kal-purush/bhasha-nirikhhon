using System.Collections;
using UnityEngine;
using TMPro;
using DG.Tweening; // Ensure DoTween is installed and imported

public class EndingController : MonoBehaviour
{
    [SerializeField] private TextMeshProUGUI EndingText1;
    [SerializeField] private TextMeshProUGUI EndingText2;
    [SerializeField] private TextMeshProUGUI EndingText3;
    [SerializeField] private TextMeshProUGUI EndingText4;

    void Start()
    {
        // Start the sequence when the game begins
        PlayEndingSequence();
    }

    private void PlayEndingSequence()
    {
        // Ensure texts are fully transparent at the start
        EndingText1.alpha = 0;
        EndingText2.alpha = 0;
        EndingText3.alpha = 0;
        EndingText4.alpha = 0;

        // Create a sequence using DoTween
        Sequence endingSequence = DOTween.Sequence();

        // Add animations for EndingText1
        endingSequence.Append(EndingText1.DOFade(1, 1f)) // Fade in over 1 second
                       .AppendInterval(2f)               // Stay visible for 2 seconds
                       .Append(EndingText1.DOFade(0, 1f)) // Fade out over 1 second
                       .OnComplete(() => EndingText1.gameObject.SetActive(false)); // Disable after fade out

        // Add animations for EndingText2 and EndingText3
        endingSequence.AppendCallback(() =>
        {
            EndingText2.gameObject.SetActive(true);
            EndingText3.gameObject.SetActive(true);
        })
        .Append(EndingText2.DOFade(1, 1f)) // Fade in Text 2
        .Join(EndingText3.DOFade(1, 1f))  // Simultaneously fade in Text 3
        .AppendInterval(3f)               // Stay visible for 3 seconds
        .Append(EndingText2.DOFade(0, 1f)) // Fade out Text 2
        .Join(EndingText3.DOFade(0, 1f))   // Simultaneously fade out Text 3
        .OnComplete(() =>
        {
            EndingText2.gameObject.SetActive(false);
            EndingText3.gameObject.SetActive(false);
        });

        // Add animations for EndingText4
        endingSequence.AppendCallback(() => EndingText4.gameObject.SetActive(true))
                       .Append(EndingText4.DOFade(1, 1f)) // Fade in Text 4
                       .AppendInterval(2f);               // Stay visible if needed

        // Start the sequence
        endingSequence.Play();
    }
}