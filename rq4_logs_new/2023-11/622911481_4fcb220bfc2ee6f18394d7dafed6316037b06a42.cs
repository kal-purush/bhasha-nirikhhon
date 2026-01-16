using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.InputSystem;
using UnityEngine.SceneManagement;

public class Escorte : TriggerScript
{
    [SerializeField] private PlayerInput playerInput;
    [SerializeField] private GameObject[] darkElves;
    [SerializeField] private Vector2 playerPosition;
    [SerializeField] private GameObject dialogueTrigger;
    [SerializeField] private int finalYPosition;
    [SerializeField] private GameObject knockOutTrigger;
    [SerializeField] private Animator crossFade;

    public override void trigger()
    {
        playerInput.enabled = false;
        playerInput.gameObject.GetComponent<Transform>().position = playerPosition;
        foreach (GameObject darkElf in darkElves)
        {
            StartCoroutine(ElfWalk(darkElf));
        }
    }

    private IEnumerator ElfWalk(GameObject darkElf)
    {
        float elapsedTime = 0;
        float morphTime = 1.5f;
        Vector2 startPosition = darkElf.transform.position;
        while (darkElf.GetComponent<Transform>().position.y < playerPosition.y - 0.1f)
        {
            darkElf.GetComponent<Transform>().position = Vector2.Lerp(startPosition,
                new Vector2(darkElf.transform.position.x, playerPosition.y), elapsedTime / morphTime);
            elapsedTime += Time.deltaTime;
            yield return new WaitForSeconds(0.01f);
        }
        dialogueTrigger.SetActive(true);
    }

    public void BringToCastle()
    {
        playerInput.gameObject.GetComponent<Animator>().SetBool("isMoving", true);
        foreach (GameObject darkElf in darkElves.Concat(new GameObject[] { playerInput.gameObject }).ToArray())
        {
            StartCoroutine(WalkToCastle(darkElf));
        }
    }

    private IEnumerator WalkToCastle(GameObject darkElf)
    {
        float morphTime = 3f;
        float elapsedTime = 0f;
        Vector2 startPosition = darkElf.transform.position;
        while (darkElf.GetComponent<Transform>().position.y > finalYPosition)
        {
            darkElf.GetComponent<Transform>().position = Vector2.Lerp(startPosition,
                new Vector2(darkElf.transform.position.x, finalYPosition), elapsedTime / morphTime);
            elapsedTime += Time.deltaTime;
            yield return null;
        }
        knockOutTrigger.SetActive(true);
    }

    public void knockOut() {
        playerInput.gameObject.GetComponent<Animator>().SetBool("isMoving", false);
    }
}