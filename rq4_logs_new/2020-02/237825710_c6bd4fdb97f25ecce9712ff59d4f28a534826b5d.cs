using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using DG.Tweening;

public class MenuController : MonoBehaviour{

    public GameObject splashScreen;

    [Header("Game Title")]
    public GameObject gameTitle;
    Vector3 gameTitleFinalPosition;

    [Header("Press Anykey Prompt")]
    public GameObject anykeyPrompt;
    Vector3 anykeyPromptFinalPosition;

    [Header("Lobby")]
    public CanvasGroup lobby; 

    private void Start() {
        StartCoroutine(MenuSequence());
        StartCoroutine(AnykeyFlash());
    }

    IEnumerator MenuSequence(){

        lobby.gameObject.SetActive(false);

        gameTitleFinalPosition = gameTitle.transform.position;
        anykeyPromptFinalPosition = anykeyPrompt.transform.position;

        gameTitle.transform.position -= Vector3.right * 500;
        anykeyPrompt.transform.position -= Vector3.right * 500;

        yield return new WaitForSeconds(4.5f);
        gameTitle.transform.DOMove(gameTitleFinalPosition, 2f).SetEase(Ease.OutExpo);

        yield return new WaitForSeconds(0.2f);
        anykeyPrompt.transform.DOMove(anykeyPromptFinalPosition, 2f).SetEase(Ease.OutExpo);

        while(true){
            yield return null;
            if (Input.anyKeyDown)
                break;
        }

        splashScreen.SetActive(false);
        lobby.gameObject.SetActive(true);
        lobby.alpha = 0f;
        lobby.DOFade(1f, 0.3f);
    }

    IEnumerator AnykeyFlash(){
        while(true){
            yield return new WaitForSeconds(1f);
            anykeyPrompt.SetActive(!anykeyPrompt.activeSelf);
        }
    }
}