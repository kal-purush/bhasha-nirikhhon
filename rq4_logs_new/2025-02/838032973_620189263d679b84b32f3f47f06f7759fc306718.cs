using UnityEngine;
using UnityEngine.SceneManagement;

public class MenuManager : MonoBehaviour
{
    [Header("Background Animation Settings")]
    public Animator backgroundAnimator;
    public string backgroundAnimationName = "BackgroundAnimation";

    [Header("Button Animation Settings")]
    public Animator[] buttonAnimators; 
    public float buttonAnimationDelay = 1f; 

    [Header("Scene Management")]
    public string nextSceneName = "GameScene"; 

    private bool[] buttonsReady; //arrray for buttons
    private bool backgroundAnimationFinished = false; 

    void Start()
    {
        buttonsReady = new bool[buttonAnimators.Length];

        if (backgroundAnimator != null) { 
            backgroundAnimator.Play(backgroundAnimationName);}

        StartCoroutine(WaitForButtonAnimations());
    }

    void Update()
    {
        if (!backgroundAnimationFinished && backgroundAnimator != null) {
            if (backgroundAnimator.GetCurrentAnimatorStateInfo(0).normalizedTime >= 0.9f) {
                backgroundAnimationFinished = true;
                PauseAnimator(backgroundAnimator); }
        }
    }
    private System.Collections.IEnumerator WaitForButtonAnimations()
    {
        yield return new WaitForSeconds(buttonAnimationDelay);

        //loop for check
        for (int i = 0; i < buttonAnimators.Length; i++)
        {
            if (buttonAnimators[i] != null) {
                while (buttonAnimators[i].GetCurrentAnimatorStateInfo(0).normalizedTime < 0.8f) {
                    yield return null;}
                buttonsReady[i] = true; 
                PauseAnimator(buttonAnimators[i]);}
        }
    }

    //stopping animator
    private void PauseAnimator(Animator animator)
    {
        animator.speed = 0; 
    }

    public void OnStartButtonClicked()
    {
        if (buttonsReady[0]) {
            SceneManager.LoadScene(nextSceneName); }
    }

    /*public void OnQuitButtonClicked()
    {
        if (buttonsReady[1]) {
            Application.Quit();}
    }*/
}